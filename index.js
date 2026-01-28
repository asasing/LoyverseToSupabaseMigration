const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

dotenv.config();

const { fetchReceiptsPage } = require("./loyverse");
const { upsertSales } = require("./supabase");

const STATE_PATH = path.join(__dirname, "state.json");

function loadState() {
  if (!fs.existsSync(STATE_PATH)) {
    return {};
  }
  try {
    const raw = fs.readFileSync(STATE_PATH, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    console.warn("Failed to read state.json, starting fresh.", err.message);
    return {};
  }
}

function saveState(state) {
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}

function isoEndOfDay(date) {
  const d = new Date(date);
  d.setUTCHours(23, 59, 59, 999);
  return d.toISOString();
}

async function run() {
  const state = loadState();

  const createdAtMin =
    state.created_at_min ||
    process.env.LOYVERSE_START_DATE ||
    "2023-08-08T00:00:00.000Z";

  const createdAtMax =
    state.created_at_max ||
    process.env.LOYVERSE_END_DATE ||
    isoEndOfDay(new Date());

  const limit = Number(process.env.LOYVERSE_PAGE_LIMIT || 200);
  const storeId = process.env.LOYVERSE_STORE_ID || null;

  let cursor = state.cursor || null;
  let page = state.page || 0;
  let totalUpserted = state.total_upserted || 0;

  console.log("Starting receipts sync");
  console.log(`Range: ${createdAtMin} -> ${createdAtMax}`);
  console.log(`Page limit: ${limit}`);

  while (true) {
    const startedAt = new Date().toISOString();

    const { receipts, cursor: nextCursor } = await fetchReceiptsPage({
      cursor,
      created_at_min: createdAtMin,
      created_at_max: createdAtMax,
      limit,
      store_id: storeId,
    });

    if (!receipts.length) {
      saveState({
        created_at_min: createdAtMin,
        created_at_max: createdAtMax,
        cursor: null,
        page,
        total_upserted: totalUpserted,
        completed_at: new Date().toISOString(),
        last_batch_count: 0,
      });
      console.log("No more receipts. Done.");
      break;
    }

    await upsertSales(receipts);

    totalUpserted += receipts.length;
    page += 1;
    cursor = nextCursor || null;

    saveState({
      created_at_min: createdAtMin,
      created_at_max: createdAtMax,
      cursor,
      page,
      total_upserted: totalUpserted,
      last_batch_count: receipts.length,
      last_batch_started_at: startedAt,
      last_batch_finished_at: new Date().toISOString(),
    });

    console.log(
      `Page ${page} ok. Batch: ${receipts.length}. Total: ${totalUpserted}.`
    );

    if (!cursor) {
      console.log("No cursor returned. Done.");
      break;
    }
  }
}

run().catch((err) => {
  console.error("Migration failed:", err);
  process.exitCode = 1;
});
