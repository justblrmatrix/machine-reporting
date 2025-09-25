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
            # Unmapped machine products (store_id is NULL)
            cur.execute("""
                SELECT DISTINCT machine_name
                FROM sales_transactions
                WHERE source <> 'POS'
                  AND machine_name IS NOT NULL
                  AND store_id IS NULL
                ORDER BY machine_name
            """)
            machine_products = cur.fetchall()

            # POS codes (to link against)
            cur.execute("""
                SELECT DISTINCT plu_code
                FROM sales_transactions
                WHERE source = 'POS'
                  AND plu_code IS NOT NULL
                ORDER BY plu_code
            """)
            pos_codes = [row["plu_code"] for row in cur.fetchall()]

            # Store IDs (valid options)
            cur.execute("""
                SELECT DISTINCT store_id
                FROM sales_transactions
                WHERE store_id IS NOT NULL
                ORDER BY store_id
            """)
            store_ids = [row["store_id"] for row in cur.fetchall()]
    finally:
        conn.close()

    return render_template(
        "mapping.html",
        machine_products=machine_products,
        pos_codes=pos_codes,
        store_ids=store_ids
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
            # Insert or reuse product mapping
            cur.execute("""
                INSERT INTO product_mapping (plu_code, machine_name, source)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (pos_code, machine_name, "mapping"))
            result = cur.fetchone()
            mapping_id = result[0] if result else None

            if not mapping_id:
                cur.execute("""
                    SELECT id FROM product_mapping
                    WHERE plu_code = %s AND machine_name = %s
                """, (pos_code, machine_name))
                mapping_id = cur.fetchone()[0]

            # Insert into product_mapping_store
            for sid in store_ids:
                cur.execute("""
                    INSERT INTO product_mapping_store (mapping_id, store_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (mapping_id, sid))

            conn.commit()
    finally:
        conn.close()

    return f"<td colspan='4'>✅ {machine_name} mapped to POS {pos_code} for stores {', '.join(map(str, store_ids))}</td>"



@app.route("/stock")
def stock():
    return render_template("stock.html")

@app.route("/variance")
def variance():
    return render_template("variance.html")

if __name__ == "__main__":
    app.run(debug=True)
