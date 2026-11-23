/**
 * Rate Limiter
 *
 * Redis-based rate limiter for Instagram private replies.
 *
 * The cap matches Meta's documented limit for this exact call: 750 private
 * replies per hour per Instagram professional account, for comments on posts
 * and reels. Exceeding it risks 429s and app-level restrictions, so the worker
 * requeues rather than pushing through.
 * https://developers.facebook.com/docs/graph-api/overview/rate-limiting/
 *
 * We run far below that ceiling: the account was checkpointed for "account
 * integrity" on 2026-11-20 while the bot fired bursts of identical DMs, so the
 * caps below are about looking human, not about Meta's API quota. Real peak was
 * 72 sent in one hour (2026-11-22). A second, per-minute bucket stops a viral
 * post from emptying the hourly allowance in one burst.
 */

import Redis from "ioredis";

const RATE_LIMIT_MAX = 40; // DMs per hour per account (Meta allows 750)
const RATE_LIMIT_WINDOW = 3600; // 1 hour in seconds
const BURST_LIMIT_MAX = 4; // DMs per minute per account
const BURST_LIMIT_WINDOW = 60;
const REQUEUE_DELAY_MS = 30 * 60 * 1000; // fallback when the window TTL is unknown
const REQUEUE_JITTER_MS = 60 * 1000; // spread requeued jobs so they don't all fire together
// Hourly requeues only; a backlog drains at RATE_LIMIT_MAX/h, so 24 tries keeps
// a comment alive for about a day before it is skipped.
const MAX_REQUEUE_ATTEMPTS = 24;

let redis: Redis | null = null;

function getRedis(): Redis {
  if (!redis) {
    redis = new Redis(process.env.REDIS_URL!, {
      maxRetriesPerRequest: null, // required by BullMQ
    });
  }
  return redis;
}

export interface RateLimitResult {
  allowed: boolean;
  currentCount: number;
  remainingDMs: number;
  shouldRequeue: boolean;
  requeueDelayMs: number;
  shouldSkip: boolean;
  reserved: boolean;
  // false for a per-minute block: the job retries in a minute without using up
  // one of its MAX_REQUEUE_ATTEMPTS.
  countsAsRequeue: boolean;
}

// Returns {allowed, hourCount, remaining, blockedBy, ttlSeconds}.
// blockedBy: 0 = allowed, 1 = hourly cap, 2 = per-minute cap.
const RESERVE_DM_SLOT_SCRIPT = `
local hour = tonumber(redis.call("GET", KEYS[1]) or "0")
local minute = tonumber(redis.call("GET", KEYS[2]) or "0")
local hour_max = tonumber(ARGV[1])
local minute_max = tonumber(ARGV[3])

if hour >= hour_max then
  return {0, hour, 0, 1, redis.call("TTL", KEYS[1])}
end
if minute >= minute_max then
  return {0, hour, hour_max - hour, 2, redis.call("TTL", KEYS[2])}
end

local next_count = redis.call("INCR", KEYS[1])
if next_count == 1 then
  redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
end
if redis.call("INCR", KEYS[2]) == 1 then
  redis.call("EXPIRE", KEYS[2], tonumber(ARGV[4]))
end

return {1, next_count, hour_max - next_count, 0, 0}
`;

function hourKey(instagramAccountId: string) {
  return `rate:dm:${instagramAccountId}`;
}

function minuteKey(instagramAccountId: string) {
  return `rate:dm:min:${instagramAccountId}`;
}

function requeueDelay(ttlSeconds: number) {
  const base = ttlSeconds > 0 ? ttlSeconds * 1000 : REQUEUE_DELAY_MS;
  return base + Math.floor(Math.random() * REQUEUE_JITTER_MS);
}

function toScriptNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") return Number.parseInt(value, 10);
  return 0;
}

function blockedResult(
  count: number,
  requeueAttempt: number,
  ttlSeconds: number = 0
): RateLimitResult {
  if (requeueAttempt >= MAX_REQUEUE_ATTEMPTS) {
    return {
      allowed: false,
      currentCount: count,
      remainingDMs: 0,
      shouldRequeue: false,
      requeueDelayMs: 0,
      shouldSkip: true,
      reserved: false,
      countsAsRequeue: true,
    };
  }

  return {
    allowed: false,
    currentCount: count,
    remainingDMs: 0,
    shouldRequeue: true,
    requeueDelayMs: requeueDelay(ttlSeconds),
    shouldSkip: false,
    reserved: false,
    countsAsRequeue: true,
  };
}

/**
 * Check if an Instagram account is within its DM rate limit.
 *
 * Uses a Redis counter with a 1-hour TTL per account.
 * Key pattern: `rate:dm:{instagramAccountId}`
 *
 * @param instagramAccountId - The Instagram account ID to check
 * @param requeueAttempt - How many times this job has been requeued (0 = first attempt)
 * @returns Rate limit result with action recommendations
 */
export async function checkRateLimit(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;

  const currentCount = await client.get(key);
  const count = currentCount ? parseInt(currentCount, 10) : 0;

  if (count >= RATE_LIMIT_MAX) {
    return blockedResult(count, requeueAttempt);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: RATE_LIMIT_MAX - count,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: false,
    countsAsRequeue: false,
  };
}

/**
 * Atomically reserve a DM send slot for an Instagram account.
 * This is the worker-safe path; it prevents concurrent jobs from all passing
 * the rate-limit check before any of them increments the Redis counter.
 */
export async function reserveDMSlot(
  instagramAccountId: string,
  requeueAttempt: number = 0
): Promise<RateLimitResult> {
  const client = getRedis();

  const result = await client.eval(
    RESERVE_DM_SLOT_SCRIPT,
    2,
    hourKey(instagramAccountId),
    minuteKey(instagramAccountId),
    RATE_LIMIT_MAX,
    RATE_LIMIT_WINDOW,
    BURST_LIMIT_MAX,
    BURST_LIMIT_WINDOW
  );
  const values = Array.isArray(result) ? result : [];
  const allowedFlag = toScriptNumber(values[0]);
  const count = toScriptNumber(values[1]);
  const remaining = toScriptNumber(values[2]);
  const blockedBy = toScriptNumber(values[3]);
  const ttlSeconds = toScriptNumber(values[4]);

  if (allowedFlag !== 1) {
    if (blockedBy === 2) {
      // Per-minute burst cap: wait out the minute, never skip for this.
      return {
        allowed: false,
        currentCount: count,
        remainingDMs: remaining,
        shouldRequeue: true,
        requeueDelayMs: requeueDelay(ttlSeconds),
        shouldSkip: false,
        reserved: false,
        countsAsRequeue: false,
      };
    }
    return blockedResult(count, requeueAttempt, ttlSeconds);
  }

  return {
    allowed: true,
    currentCount: count,
    remainingDMs: remaining,
    shouldRequeue: false,
    requeueDelayMs: 0,
    shouldSkip: false,
    reserved: true,
    countsAsRequeue: false,
  };
}

/**
 * Release a DM slot previously taken by reserveDMSlot.
 *
 * reserveDMSlot increments the hourly counter before the send, so concurrent
 * jobs can't all pass the check at once. When that send then fails (closed
 * messaging window, expired token, rejected reply) the reserved slot is never
 * used and must be handed back. Otherwise a comment that never delivers a DM
 * still burns one slot per attempt, and BullMQ's retries burn several. On a
 * post with many failing sends the counter inflates past the real number of
 * DMs and legitimate replies get skipped as rate-limited until the TTL expires.
 *
 * DECR is atomic and preserves the key's TTL, so the hourly window still resets
 * when it originally would. A missing key (the window already rolled over)
 * would decrement to -1 with no expiry, so that case is clamped back to zero.
 */
export async function releaseDMSlot(
  instagramAccountId: string
): Promise<number> {
  // Only the hourly slot is handed back; the minute slot just expires.
  const client = getRedis();
  const key = hourKey(instagramAccountId);
  const next = await client.decr(key);
  if (next < 0) {
    await client.del(key);
    return 0;
  }
  return next;
}

/**
 * Backwards-compatible helper for tests and admin scripts.
 * Prefer reserveDMSlot in workers.
 */
export async function incrementDMCounter(
  instagramAccountId: string
): Promise<number> {
  const result = await reserveDMSlot(instagramAccountId, MAX_REQUEUE_ATTEMPTS);
  return result.currentCount;
}

/**
 * Get the current DM count for an Instagram account.
 */
export async function getCurrentDMCount(
  instagramAccountId: string
): Promise<number> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  const count = await client.get(key);
  return count ? parseInt(count, 10) : 0;
}

/**
 * Reset the rate limiter for an account (useful for testing).
 */
export async function resetRateLimit(
  instagramAccountId: string
): Promise<void> {
  const client = getRedis();
  const key = `rate:dm:${instagramAccountId}`;
  await client.del(key);
}

// Export constants for use in tests
export {
  RATE_LIMIT_MAX,
  RATE_LIMIT_WINDOW,
  BURST_LIMIT_MAX,
  BURST_LIMIT_WINDOW,
  REQUEUE_DELAY_MS,
  MAX_REQUEUE_ATTEMPTS,
};
