import os
from flask import Flask, render_template, request
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

def get_db_conn():
    return psycopg2.connect(DATABASE_URL)

app = Flask(__name__)

@app.route("/")
def dashboard():
    """Show last 10 transactions."""
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, source, store_id, device_id, date, time, transaction_id,
                       plu_code, product_name, machine_name, quantity, amount, currency, created_at
                FROM public.sales_transactions
                ORDER BY created_at DESC
                LIMIT 10
            """)
            transactions = cur.fetchall()
    finally:
        conn.close()

    return render_template("dashboard.html", transactions=transactions)

@app.route("/mapping")
def mapping():
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Machine products not mapped
            cur.execute("""
                SELECT DISTINCT st.machine_name
                FROM sales_transactions st
                WHERE st.source <> 'POS'
                  AND st.machine_name IS NOT NULL
                  AND st.store_id IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM product_mapping pm
                    WHERE pm.machine_name = st.machine_name
                  )
                ORDER BY st.machine_name
            """)
            unmapped_machines = cur.fetchall()

            # Store IDs (still needed)
            cur.execute("""
                SELECT DISTINCT store_id
                FROM sales_transactions
                WHERE store_id IS NOT NULL
                ORDER BY store_id
            """)
            store_ids = [row["store_id"] for row in cur.fetchall()]

            # Already mapped
            cur.execute("""
                SELECT pm.id, pm.machine_name, pm.plu_code,
                       array_agg(pms.store_id ORDER BY pms.store_id) AS stores
                FROM product_mapping pm
                LEFT JOIN product_mapping_store pms ON pm.id = pms.mapping_id
                GROUP BY pm.id, pm.machine_name, pm.plu_code
                ORDER BY pm.machine_name
            """)
            mapped = cur.fetchall()
    finally:
        conn.close()

    return render_template(
        "mapping.html",
        unmapped_machines=unmapped_machines,
        store_ids=store_ids,
        mapped=mapped
    )





@app.route("/mapping/add", methods=["POST"])
def add_mapping():
    machine_name = request.form.get("machine_name")
    pos_code = request.form.get("pos_code")
    store_ids = request.form.get("store_ids", "")

    if not machine_name or not pos_code or not store_ids:
        return "<td colspan='4'>❌ Missing data</td>"

    store_ids = [int(s) for s in store_ids.split(",") if s.strip()]

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Insert mapping (machine ↔ pos)
            cur.execute("""
                INSERT INTO product_mapping (plu_code, machine_name, source)
                VALUES (%s, %s, 'mapping')
                ON CONFLICT (machine_name, plu_code)
                DO UPDATE SET updated_at = now()
                RETURNING id
            """, (pos_code, machine_name))
            mapping_id = cur.fetchone()[0]

            # Link to stores
            for sid in store_ids:
                cur.execute("""
                    INSERT INTO product_mapping_store (mapping_id, store_id)
                    VALUES (%s, %s)
                    ON CONFLICT (mapping_id, store_id) DO NOTHING
                """, (mapping_id, sid))

            conn.commit()
    finally:
        conn.close()

    return f"<td colspan='4'>✅ {machine_name} → POS {pos_code} @ stores {', '.join(map(str,store_ids))}</td>"




from datetime import date

@app.route("/stock", methods=["GET", "POST"])
def stock():
    if request.method == "POST":
        # ... existing code ...
        return redirect(url_for("stock"))

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM daily_stock
                ORDER BY date DESC, store_id, device_id, plu_code
                LIMIT 50
            """)
            entries = cur.fetchall()

            cur.execute("SELECT DISTINCT store_id FROM sales_transactions WHERE store_id IS NOT NULL ORDER BY store_id")
            stores = [row["store_id"] for row in cur.fetchall()]

            cur.execute("SELECT DISTINCT device_id FROM sales_transactions WHERE device_id IS NOT NULL ORDER BY device_id")
            devices = [row["device_id"] for row in cur.fetchall()]

            cur.execute("SELECT DISTINCT plu_code FROM sales_transactions WHERE plu_code IS NOT NULL ORDER BY plu_code")
            plu_codes = [row["plu_code"] for row in cur.fetchall()]
    finally:
        conn.close()

    return render_template(
        "stock.html",
        entries=entries,
        stores=stores,
        devices=devices,
        plu_codes=plu_codes,
        today=date.today().isoformat()
    )


@app.route("/variance", methods=["GET", "POST"])
def variance():
    store_id = request.args.get("store_id")
    date_str = request.args.get("date")

    rows = []
    if store_id and date_str:
        conn = get_db_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                WITH sales AS (
  SELECT pm.plu_code,
         SUM(st.quantity) FILTER (WHERE st.source='POS') AS pos_qty,
         SUM(st.quantity) FILTER (WHERE st.source IN ('Nozzle','Robobar')) AS machine_qty
  FROM sales_transactions st
  LEFT JOIN product_mapping pm
    ON (st.machine_name = pm.machine_name OR st.plu_code = pm.plu_code)
  LEFT JOIN product_mapping_store pms
    ON pm.id = pms.mapping_id
  WHERE st.date = %s
    AND pms.store_id = %s
  GROUP BY pm.plu_code
),
yest AS (
  SELECT plu_code, closing
  FROM daily_stock
  WHERE date = %s::date - interval '1 day'
    AND store_id = %s
),
today AS (
  SELECT plu_code, replenishment, closing, note
  FROM daily_stock
  WHERE date = %s AND store_id = %s
)
SELECT COALESCE(yest.closing,0) AS opening,
       COALESCE(today.replenishment,0) AS replenishment,
       COALESCE(sales.pos_qty,0) AS pos_sales,
       COALESCE(sales.machine_qty,0) AS machine_sales,
       (COALESCE(yest.closing,0)+COALESCE(today.replenishment,0) 
         - (COALESCE(sales.pos_qty,0)+COALESCE(sales.machine_qty,0))) AS expected_closing,
       today.closing AS physical_closing,
       (COALESCE(today.closing,0) - 
        (COALESCE(yest.closing,0)+COALESCE(today.replenishment,0) 
          - (COALESCE(sales.pos_qty,0)+COALESCE(sales.machine_qty,0)))) AS variance,
       sales.plu_code
FROM sales
FULL JOIN yest ON yest.plu_code = sales.plu_code
FULL JOIN today ON today.plu_code = COALESCE(sales.plu_code,yest.plu_code);

                """, (date_str, store_id, date_str, store_id, date_str, store_id))
                rows = cur.fetchall()
        finally:
            conn.close()

    # stores for dropdown
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT store_id FROM sales_transactions WHERE store_id IS NOT NULL ORDER BY store_id")
            stores = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    return render_template("variance.html",
                           rows=rows,
                           stores=stores,
                           store_id=store_id,
                           date=date_str)


if __name__ == "__main__":
    app.run(debug=True)
