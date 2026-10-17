// `next dev` loads .env for the web app; tsx does not, so the worker would
// start without DATABASE_URL/REDIS_URL. Imported first because some modules read
// env at import time. Never overrides variables already set (Docker, a VM, etc.).
try {
  process.loadEnvFile();
} catch {
  // No .env file: the environment is provided some other way.
}
