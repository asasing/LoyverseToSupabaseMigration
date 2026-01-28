const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

function toNumber(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function pickReceiptDate(r) {
  const dt = r.receipt_date || r.created_at || r.date || null;
  if (!dt) return { receipt_date: null, receipt_datetime: null };
  const iso = new Date(dt).toISOString();
  return {
    receipt_date: iso.split("T")[0],
    receipt_datetime: iso,
  };
}

async function upsertSales(receipts) {
  if (!receipts.length) return;

  const rows = receipts
    .map((r) => {
    const receiptNumber = r.receipt_number || r.receipt_id || r.id || null;
    const receiptId = receiptNumber || r.id || r.receipt_id || null;
    const totalMoney = toNumber(r.total_money);
    const totalDiscount = toNumber(r.total_discount);
    const refundMoney = toNumber(r.refund_money);
    const cost = toNumber(r.cost);
    const netSales = totalMoney - totalDiscount - refundMoney;

    const { receipt_date, receipt_datetime } = pickReceiptDate(r);

    const row = {
      receipt_id: receiptId,
      receipt_number: receiptNumber,
      store_id: r.store_id || null,
      device_id: r.device_id || null,
      employee_id: r.employee_id || null,
      customer_id: r.customer_id || null,
      receipt_type: r.receipt_type || null,
      state: r.state || null,
      source: r.source || null,
      receipt_datetime,
      receipt_date,
      gross_sales: totalMoney,
      discounts: totalDiscount,
      refunds: refundMoney,
      taxes: toNumber(r.total_tax),
      net_sales: netSales,
      cost_of_goods: cost,
      gross_profit: netSales - cost,
      currency: r.currency || null,
      raw: r,
    };
    if (r.created_at) {
      row.created_at = r.created_at;
    }
    return row;
  })
    .filter((row) => row.receipt_id);

  const CHUNK = Number(process.env.SUPABASE_UPSERT_CHUNK || 500);

  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);

    const { error } = await supabase
      .from("sales")
      .upsert(chunk, { onConflict: "receipt_id" });

    if (error) throw error;

    console.log(`Upserted ${i + chunk.length}/${rows.length}`);
  }
}

module.exports = { upsertSales };
