import os
from flask import Flask, render_template
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

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

# Placeholder routes
@app.route("/mapping")
def mapping():
    return render_template("mapping.html")

@app.route("/stock")
def stock():
    return render_template("stock.html")

@app.route("/variance")
def variance():
    return render_template("variance.html")

if __name__ == "__main__":
    app.run(debug=True)
