from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
from datetime import datetime, date

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "fallbacksecret")

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    url = urlparse(DATABASE_URL)
    return psycopg2.connect(
        host=url.hostname,
        dbname=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port
    )

# ---------------------------
# Dashboard
# ---------------------------
@app.route("/")
def dashboard():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
        SELECT date, time, source, device_id, plu_code, product_name, quantity, amount
        FROM sales_transactions
        ORDER BY date DESC, time DESC
        LIMIT 20
    """)
    txns = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("dashboard.html", txns=txns)


# ---------------------------
# Mapping
# ---------------------------
@app.route("/mapping/33nozzle", methods=["GET", "POST"])
def mapping_nozzle():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == "POST":
        store_id = request.form.get("store_id")
        plu_code = request.form.get("plu_code")

        ingredients = request.form.getlist("ingredient_name[]")
        volumes = request.form.getlist("volume[]")

        for ing, vol in zip(ingredients, volumes):
            if not ing.strip() or not vol.strip():
                continue
            cur.execute("""
                INSERT INTO nozzle_mapping (store_id, plu_code, ingredient_name, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (store_id, plu_code, ingredient_name) DO UPDATE
                SET volume = EXCLUDED.volume
            """, (store_id, plu_code, ing.strip(), float(vol)))

        conn.commit()
        flash(f"✅ Mapping saved for {plu_code} (store {store_id})", "success")
        return redirect(url_for("mapping_nozzle"))

    # Stores
    cur.execute("SELECT DISTINCT store_id FROM sales_transactions WHERE store_id IS NOT NULL ORDER BY store_id")
    stores = [row[0] for row in cur.fetchall()]

    # Unmapped PLUs
    cur.execute("""
        SELECT DISTINCT st.plu_code, st.product_name, st.store_id
        FROM sales_transactions st
        WHERE st.source = 'POS'
          AND NOT EXISTS (
              SELECT 1 FROM nozzle_mapping m
              WHERE m.plu_code = st.plu_code
              AND m.store_id = st.store_id
          )
        ORDER BY st.plu_code
        LIMIT 100
    """)
    unmapped = cur.fetchall()

    # Existing mappings
    cur.execute("""
        SELECT store_id, plu_code, ingredient_name, volume, created_at
        FROM nozzle_mapping
        ORDER BY store_id, plu_code, ingredient_name
    """)
    mappings = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("mapping_nozzle.html",
                           stores=stores,
                           unmapped=unmapped,
                           mappings=mappings)



# ---------------------------
# Stock
# ---------------------------
@app.route("/stock", methods=["GET", "POST"])
def stock():
    conn = get_conn()
    cur = conn.cursor()

    if request.method == "POST":
        store_id = request.form.get("store_id")
        date = request.form.get("date")
        ingredient_names = request.form.getlist("ingredient_name[]")
        replenishments = request.form.getlist("replenishment[]")

        for ing, rep in zip(ingredient_names, replenishments):
            rep = float(rep or 0)
            cur.execute("""
                INSERT INTO daily_stock (store_id, date, ingredient_name, replenishment)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (store_id, date, ingredient_name) DO UPDATE
                SET replenishment = EXCLUDED.replenishment
            """, (store_id, date, ing, rep))

        conn.commit()
        flash(f"✅ Replenishment saved for {date} (Store {store_id})", "success")
        return redirect(url_for("stock", store_id=store_id))

    # list available stores
    cur.execute("SELECT DISTINCT store_id FROM sales_transactions WHERE store_id IS NOT NULL ORDER BY store_id")
    stores = [row[0] for row in cur.fetchall()]

    # get selected store
    store_id = request.args.get("store_id")

    ingredients = []
    if store_id:
        cur.execute("""
            SELECT DISTINCT ingredient_name
            FROM nozzle_mapping
            WHERE store_id = %s
            ORDER BY ingredient_name
        """, (store_id,))
        ingredients = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()
    return render_template("stock.html", stores=stores, ingredients=ingredients, selected_store=store_id)


@app.route("/closing", methods=["GET", "POST"])
def closing():
    conn = get_conn()
    cur = conn.cursor()

    if request.method == "POST":
        store_id = request.form.get("store_id")
        date = request.form.get("date")
        secret = request.form.get("secret")

        if secret != os.getenv("CLOSING_SECRET", "letmein"):
            flash("❌ Invalid secret phrase", "danger")
            return redirect(url_for("closing", store_id=store_id))

        ingredient_names = request.form.getlist("ingredient_name[]")
        closings = request.form.getlist("closing[]")

        for ing, clo in zip(ingredient_names, closings):
            clo = float(clo or 0)
            cur.execute("""
                INSERT INTO daily_stock (store_id, date, ingredient_name, closing)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (store_id, date, ingredient_name) DO UPDATE
                SET closing = EXCLUDED.closing
            """, (store_id, date, ing, clo))

        conn.commit()
        flash(f"✅ Closing saved for {date} (Store {store_id})", "success")
        return redirect(url_for("closing", store_id=store_id))

    # list available stores
    cur.execute("SELECT DISTINCT store_id FROM sales_transactions WHERE store_id IS NOT NULL ORDER BY store_id")
    stores = [row[0] for row in cur.fetchall()]

    # get selected store
    store_id = request.args.get("store_id")

    ingredients = []
    if store_id:
        cur.execute("""
            SELECT DISTINCT ingredient_name
            FROM nozzle_mapping
            WHERE store_id = %s
            ORDER BY ingredient_name
        """, (store_id,))
        ingredients = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()
    return render_template("closing.html", stores=stores, ingredients=ingredients, selected_store=store_id)



# ---------------------------
# Variance
# ---------------------------
@app.route("/variance", methods=["GET", "POST"])
def variance():
    rows = []
    device_id = None
    date = None
    if request.method == "POST":
        device_id = request.form["device_id"]
        date = request.form["date"]

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
        WITH expanded_sales AS (
            SELECT
                m.ingredient_name,
                st.device_id,
                SUM(st.quantity * m.volume) AS consumed
            FROM sales_transactions st
            JOIN manual_mapping m
              ON (
                   (st.source = 'POS' AND st.plu_code = m.plu_code)
                OR (st.source IN ('Nozzle','Robobar') AND lower(st.machine_name) = lower(m.machine_name))
              )
             AND (m.device_id IS NULL OR m.device_id = st.device_id)
            WHERE st.date = %s
              AND st.device_id = %s
              AND m.active = true
            GROUP BY m.ingredient_name, st.device_id
        ),
        yesterday AS (
            SELECT ingredient_name, closing
            FROM daily_stock
            WHERE date = %s::date - interval '1 day'
              AND device_id = %s
        ),
        today AS (
            SELECT ingredient_name, replenishment, closing
            FROM daily_stock
            WHERE date = %s
              AND device_id = %s
        )
        SELECT
            COALESCE(yesterday.closing, 0) AS opening,
            COALESCE(today.replenishment, 0) AS replenishment,
            COALESCE(expanded_sales.consumed, 0) AS consumed,
            (COALESCE(yesterday.closing, 0) + COALESCE(today.replenishment, 0) - COALESCE(expanded_sales.consumed, 0)) AS expected_closing,
            today.closing AS physical_closing,
            (COALESCE(today.closing, 0) -
             (COALESCE(yesterday.closing, 0) + COALESCE(today.replenishment, 0) - COALESCE(expanded_sales.consumed, 0))) AS variance,
            COALESCE(expanded_sales.ingredient_name, yesterday.ingredient_name, today.ingredient_name) AS ingredient_name
        FROM expanded_sales
        FULL JOIN yesterday ON yesterday.ingredient_name = expanded_sales.ingredient_name
        FULL JOIN today ON today.ingredient_name = COALESCE(expanded_sales.ingredient_name, yesterday.ingredient_name);
        """
        cur.execute(query, (date, device_id, date, device_id, date, device_id))
        rows = cur.fetchall()

        cur.close()
        conn.close()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT device_id FROM sales_transactions WHERE device_id IS NOT NULL")
    devices = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    return render_template("variance.html", rows=rows, devices=devices, device_id=device_id, date=date)

@app.route("/variance/nozzle", methods=["GET", "POST"])
def variance_nozzle():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Pick date (default = today)
    selected_date = request.form.get("date") or date.today().strftime("%Y-%m-%d")

    query = """
        WITH pos_sales AS (
            SELECT
                st.date,
                nm.ingredient_name,
                SUM(st.quantity * nm.volume) AS pos_consumed
            FROM sales_transactions st
            JOIN nozzle_mapping nm
              ON st.plu_code = nm.plu_code
             AND st.store_id = nm.store_id
            WHERE st.source = 'POS'
              AND st.date = %s
            GROUP BY st.date, nm.ingredient_name
        ),
        machine_sales AS (
            SELECT
                st.date,
                nm.ingredient_name,
                SUM(st.quantity * nm.volume) AS machine_consumed
            FROM sales_transactions st
            JOIN nozzle_mapping nm
              ON st.machine_name = nm.machine_name
             AND st.store_id = nm.store_id
            WHERE st.source = 'Nozzle'
              AND st.date = %s
            GROUP BY st.date, nm.ingredient_name
        ),
        stock_movements AS (
            SELECT
                ds.date,
                ds.ingredient_name,
                COALESCE(LAG(ds.closing) OVER (PARTITION BY ds.ingredient_name ORDER BY ds.date),0) AS opening,
                ds.replenishment,
                ds.closing
            FROM daily_stock ds
            WHERE ds.date <= %s
        )
        SELECT
            COALESCE(ps.date, ms.date, sm.date) AS date,
            COALESCE(ps.ingredient_name, ms.ingredient_name, sm.ingredient_name) AS ingredient_name,
            COALESCE(sm.opening,0) AS opening,
            COALESCE(sm.replenishment,0) AS replenishment,
            COALESCE(ps.pos_consumed,0) AS pos_sales,
            COALESCE(ms.machine_consumed,0) AS machine_sales,
            (COALESCE(sm.opening,0) + COALESCE(sm.replenishment,0)
             - COALESCE(ps.pos_consumed,0) - COALESCE(ms.machine_consumed,0)) AS expected_closing,
            COALESCE(sm.closing,0) AS physical_closing,
            (COALESCE(ps.pos_consumed,0) - COALESCE(ms.machine_consumed,0)) AS variance
        FROM pos_sales ps
        FULL OUTER JOIN machine_sales ms
          ON ps.date = ms.date AND ps.ingredient_name = ms.ingredient_name
        FULL OUTER JOIN stock_movements sm
          ON COALESCE(ps.date, ms.date) = sm.date
         AND COALESCE(ps.ingredient_name, ms.ingredient_name) = sm.ingredient_name
        ORDER BY ingredient_name;
    """
    cur.execute(query, (selected_date, selected_date, selected_date))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("variance_nozzle.html",
                           rows=rows,
                           selected_date=selected_date)




if __name__ == "__main__":
    app.run(debug=True)
