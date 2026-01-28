const axios = require("axios");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildAuthHeader() {
  const explicit = process.env.LOYVERSE_AUTH_HEADER;
  if (explicit) return explicit;
  const token =
    process.env.LOYVERSE_API_KEY || process.env.LOYVERSE_ACCESS_TOKEN || "";
  if (!token) {
    throw new Error("Missing LOYVERSE_API_KEY (or LOYVERSE_ACCESS_TOKEN).");
  }
  return token.startsWith("Bearer ") ? token : `Bearer ${token}`;
}

function normalizeReceiptsPayload(data) {
  if (!data) return { receipts: [], cursor: null };
  if (Array.isArray(data)) {
    return { receipts: data, cursor: null };
  }
  const receipts =
    data.receipts || data.items || data.data || data.results || [];
  const cursor =
    data.cursor || data.next_cursor || data.next || data.nextCursor || null;
  return { receipts, cursor };
}

async function requestWithRetries(url, config, maxAttempts = 6) {
  let attempt = 0;
  let lastError;

  while (attempt < maxAttempts) {
    attempt += 1;
    try {
      return await axios.get(url, config);
    } catch (err) {
      lastError = err;
      const status = err.response?.status;
      const isRetryable =
        status === 429 || (status && status >= 500) || err.code === "ECONNABORTED";
      if (!isRetryable || attempt >= maxAttempts) {
        throw err;
      }

      const retryAfterHeader = err.response?.headers?.["retry-after"];
      const retryAfterMs = retryAfterHeader
        ? Number(retryAfterHeader) * 1000
        : 0;
      const backoffMs = Math.min(30000, 500 * 2 ** (attempt - 1));
      const waitMs = Math.max(retryAfterMs, backoffMs);
      console.warn(`Retrying (${attempt}/${maxAttempts}) after ${waitMs}ms`);
      await sleep(waitMs);
    }
  }

  throw lastError;
}

async function fetchReceiptsPage({
  cursor,
  created_at_min,
  created_at_max,
  limit,
  store_id,
}) {
  const baseUrl = process.env.LOYVERSE_API_BASE || "https://api.loyverse.com/v1.0";
  const url = `${baseUrl.replace(/\/$/, "")}/receipts`;
  const headers = { Authorization: buildAuthHeader() };

  const params = {
    limit,
  };
  if (cursor) params.cursor = cursor;
  if (created_at_min) params.created_at_min = created_at_min;
  if (created_at_max) params.created_at_max = created_at_max;
  if (store_id) params.store_id = store_id;

  const response = await requestWithRetries(url, {
    headers,
    params,
    timeout: Number(process.env.LOYVERSE_HTTP_TIMEOUT_MS || 60000),
  });

  const { receipts, cursor: nextCursor } = normalizeReceiptsPayload(
    response.data
  );

  const sleepMs = Number(process.env.LOYVERSE_SLEEP_MS || 1200);
  if (sleepMs > 0) {
    await sleep(sleepMs);
  }

  return { receipts, cursor: nextCursor };
}

module.exports = { fetchReceiptsPage };
