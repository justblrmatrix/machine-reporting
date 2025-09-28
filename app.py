from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
from datetime import datetime, date, timedelta
from psycopg2.extras import RealDictCursor
import re

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
        SELECT id, store_id, plu_code, ingredient_name, volume, created_at
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


@app.route("/mapping/33nozzle/delete", methods=["POST"])
def delete_mappings_nozzle():
    ids = request.form.getlist("ids[]")  # comes from checkboxes
    if ids:
        conn = get_conn()
        cur = conn.cursor()
        # cast ids to integers
        ids = [int(x) for x in ids]
        cur.execute("DELETE FROM nozzle_mapping WHERE id = ANY(%s)", (ids,))
        conn.commit()
        cur.close()
        conn.close()
        flash(f"❌ Deleted {len(ids)} mappings", "warning")
    else:
        flash("⚠️ No mappings selected for deletion", "danger")

    return redirect(url_for("mapping_nozzle"))



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

def normalize_name(raw):
    if not raw:
        return ""
    s = raw.lower()
    s = re.sub(r'[\r\n\t]+', ' ', s)          # remove line breaks/tabs
    s = re.sub(r"['`´]", "", s)               # remove apostrophes/backticks
    s = re.sub(r'[^a-z0-9&]+', ' ', s)        # keep only alnum + &
    s = re.sub(r'\s+', ' ', s).strip()        # collapse spaces
    return s

@app.route("/variance/nozzle", methods=["GET", "POST"])
def variance_nozzle():
    from psycopg2.extras import RealDictCursor
    from statistics import mode
    import re
    from datetime import datetime, date, timedelta
    from flask import Response
    import csv
    from io import StringIO

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # --- Inputs ---
    selected_date = request.form.get("date") or request.args.get("date") or date.today().strftime("%Y-%m-%d")
    d = datetime.strptime(selected_date, "%Y-%m-%d").date()
    d_prev = d - timedelta(days=1)

    def normalize_name(s: str) -> str:
        if not s:
            return ""
        # collapse whitespace, remove non-alphanumerics, lowercase
        return re.sub(r"[^a-z0-9]+", "", " ".join(s.split()).lower())

    # --- Load mappings ---
    cur.execute("""
        SELECT store_id, plu_code, machine_name, ingredient_name, volume
        FROM nozzle_mapping
        WHERE active = true
    """)
    nm = cur.fetchall()

    # Direct POS mapping
    map_plu_direct = {}
    for r in nm:
        if r["plu_code"]:
            map_plu_direct[(r["plu_code"], r["store_id"])] = (r["ingredient_name"], float(r["volume"] or 0))

    # Machine mapping (store both exact and normalized machine names)
    map_machine = {}
    for r in nm:
        raw = (r["machine_name"] or "").strip()
        store = r["store_id"]
        mapping_entry = (r["ingredient_name"], float(r["volume"] or 0))

        key_exact = (raw, store)
        key_norm = (normalize_name(raw), store)

        for k in [key_exact, key_norm]:
            map_machine.setdefault(k, []).append(mapping_entry)

    # Ingredient unit size
    per_ing_unit_ml = {}
    by_ing_store = {}
    for r in nm:
        ing, store, vol = r["ingredient_name"], r["store_id"], float(r["volume"] or 0)
        if vol > 0 and ing:
            by_ing_store.setdefault((ing, store), []).append(vol)
    for key, vols in by_ing_store.items():
        try:
            per_ing_unit_ml[key] = float(mode(vols))
        except Exception:
            per_ing_unit_ml[key] = 30.0

    # --- Cocktail recipes ---
    cur.execute("""
        SELECT store_id, cocktail_plu, ingredient_name, volume_ml
        FROM cocktail_recipes
        WHERE active = true
    """)
    recipes_rows = cur.fetchall()
    recipes = {}
    for r in recipes_rows:
        recipes.setdefault((r["cocktail_plu"], r["store_id"]), []).append(
            (r["ingredient_name"], float(r["volume_ml"]))
        )

    # --- POS sales ---
    cur.execute("""
        SELECT st.plu_code, st.store_id, st.quantity
        FROM sales_transactions st
        WHERE st.source = 'POS' AND st.date = %s
    """, (d,))
    pos_rows = cur.fetchall()

    pos_units, pos_ml, contrib_map = {}, {}, {}

    for r in pos_rows:
        plu, store, qty = r["plu_code"], r["store_id"], float(r["quantity"] or 0)

        # Pure ingredient PLU
        direct = map_plu_direct.get((plu, store))
        if direct:
            ing, ml_per_unit = direct
            if ml_per_unit > 0:
                pos_units[ing] = pos_units.get(ing, 0.0) + qty
                pos_ml[ing] = pos_ml.get(ing, 0.0) + qty * ml_per_unit
            continue

        # Cocktail PLU
        rec = recipes.get((plu, store))
        if rec:
            for ing, ml in rec:
                pos_ml[ing] = pos_ml.get(ing, 0.0) + qty * ml
                unit_ml = per_ing_unit_ml.get((ing, store), 30.0)
                units_equiv = (qty * ml / unit_ml)
                pos_units[ing] = pos_units.get(ing, 0.0) + units_equiv
                contrib_map.setdefault(ing, []).append(
                    f"{qty} × {plu} → {ml*qty:.0f} ml ({units_equiv:.1f} units)"
                )

    # --- Machine sales ---
    cur.execute("""
        SELECT machine_name, quantity, store_id
        FROM sales_transactions
        WHERE source = 'Nozzle' AND date = %s
    """, (d,))
    noz_rows = cur.fetchall()

    machine_units = {}
    for r in noz_rows:
        raw_name = (r["machine_name"] or "").strip()
        store = r["store_id"]
        qty_total_ml = float(r["quantity"] or 0)

        key_exact = (raw_name, store)
        key_norm = (normalize_name(raw_name), store)

        mappings = []
        if store is None:
            # No store_id → allow match against any store’s mapping
            for (mname, mstore), vals in map_machine.items():
                if mname == raw_name or mname == normalize_name(raw_name):
                    mappings.extend(vals)
        else:
            if key_exact in map_machine:
                mappings = map_machine[key_exact]
            elif key_norm in map_machine:
                mappings = map_machine[key_norm]

        if not mappings:
            continue

        # --- FIX: Treat quantity as full drink size ---
        base_size = sum(vol for _, vol in mappings if vol > 0)
        if base_size <= 0:
            continue

        n_servings = qty_total_ml / base_size

        for ing, vol in mappings:
            if vol > 0:
                used_ml = n_servings * vol
                unit_ml = per_ing_unit_ml.get((ing, store), 30.0)
                machine_units[ing] = machine_units.get(ing, 0.0) + (used_ml / unit_ml)

    # --- Stock (all stores aggregated) ---
    cur.execute("""
        SELECT ingredient_name,
               SUM(CASE WHEN date = %s THEN closing ELSE 0 END) AS opening,
               SUM(CASE WHEN date = %s THEN replenishment ELSE 0 END) AS replenishment,
               SUM(CASE WHEN date = %s THEN closing ELSE 0 END) AS closing
        FROM daily_stock
        WHERE date IN (%s, %s)
        GROUP BY ingredient_name
    """, (d_prev, d, d, d_prev, d))
    stock_rows = {r["ingredient_name"]: r for r in cur.fetchall()}

    # --- Final rows ---
    ingredients = set(stock_rows) | set(pos_units) | set(machine_units)
    rows = []
    for ing in sorted(ingredients):
        s = stock_rows.get(ing, {})
        opening = float(s.get("opening") or 0.0)
        replenishment = float(s.get("replenishment") or 0.0)
        closing = float(s.get("closing") or 0.0)

        pos_units_val = float(pos_units.get(ing, 0.0))
        pos_ml_val = float(pos_ml.get(ing, 0.0))
        machine_units_val = float(machine_units.get(ing, 0.0))

        expected_closing = opening + replenishment - pos_ml_val
        variance_units = pos_units_val - machine_units_val

        rows.append({
            "ingredient_name": ing,
            "opening": round(opening, 2),
            "replenishment": round(replenishment, 2),
            "pos_sales": round(pos_units_val, 2),
            "machine_sales": round(machine_units_val, 2),
            "expected_closing": round(expected_closing, 2),
            "physical_closing": round(closing, 2),
            "variance": round(variance_units, 2),
            "details": contrib_map.get(ing, [])
        })

    cur.close()
    conn.close()

    # --- CSV export ---
    if request.args.get("export") == "csv":
        si = StringIO()
        cw = csv.DictWriter(si, fieldnames=["ingredient_name", "opening", "replenishment", "pos_sales", "machine_sales", "expected_closing", "physical_closing", "variance"])
        cw.writeheader()
        cw.writerows(rows)
        return Response(
            si.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment;filename=variance_{selected_date}.csv"}
        )

    return render_template("variance_nozzle.html", rows=rows, selected_date=selected_date)
 


@app.route("/mapping/robobar", methods=["GET", "POST"])
def mapping_robobar():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == "POST":
        machine_id = request.form.get("machine_id")
        plu_code = request.form.get("plu_code")
        digitory_name = request.form.get("digitory_name")
        machine_name = request.form.get("machine_name")
        store_ids = request.form.getlist("store_ids")  # multiple

        for sid in store_ids:
            cur.execute("""
                INSERT INTO robobar_mapping (machine_id, store_id, plu_code, digitory_name, machine_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (store_id, plu_code) DO UPDATE
                SET machine_id = EXCLUDED.machine_id,
                    digitory_name = EXCLUDED.digitory_name,
                    machine_name = EXCLUDED.machine_name
            """, (machine_id, sid, plu_code, digitory_name, machine_name))

        conn.commit()
        flash(f"✅ Robobar mapping saved for {plu_code}", "success")
        return redirect(url_for("mapping_robobar"))

    # Existing mappings
    cur.execute("""
        SELECT * FROM robobar_mapping
        ORDER BY store_id, plu_code
    """)
    mappings = cur.fetchall()

    # Distinct stores for selection
    cur.execute("SELECT DISTINCT store_id FROM sales_transactions WHERE store_id IS NOT NULL ORDER BY store_id")
    stores = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return render_template("mapping_robobar.html", mappings=mappings, stores=stores)

@app.route("/mapping/robobar/delete", methods=["POST"])
def delete_robobar_mappings():
    ids = request.form.getlist("ids")
    if not ids:
        flash("⚠️ No mappings selected for deletion", "warning")
        return redirect(url_for("mapping_robobar"))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM robobar_mapping WHERE id = ANY(%s)", (ids,))
    conn.commit()
    cur.close()
    conn.close()

    flash(f"❌ Deleted {len(ids)} mappings", "warning")
    return redirect(url_for("mapping_robobar"))


@app.route("/variance/robobar", methods=["GET", "POST"])
def variance_robobar():
    from psycopg2.extras import RealDictCursor
    import re
    from datetime import datetime, date

    def normalize_name(name: str) -> str:
        """Aggressive normalization: lowercase, remove all non-alphanumerics."""
        return re.sub(r"[^a-z0-9]+", "", name.lower()) if name else ""

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Selected date
    selected_date = request.form.get("date") or date.today().strftime("%Y-%m-%d")
    d = datetime.strptime(selected_date, "%Y-%m-%d").date()

    # --- Load robobar mappings ---
    cur.execute("""
        SELECT DISTINCT plu_code, machine_name
        FROM robobar_mapping
    """)
    mappings = cur.fetchall()

    # Map plu_code -> normalized machine_name + pretty display
    mapping_dict = {}
    for row in mappings:
        norm_mname = normalize_name(row["machine_name"])
        mapping_dict[row["plu_code"]] = {
            "machine_name_display": row["machine_name"],
            "machine_name_norm": norm_mname
        }

    # --- POS sales (cluster level) ---
    cur.execute("""
        SELECT plu_code, SUM(quantity) as qty
        FROM sales_transactions
        WHERE source = 'POS' AND date = %s
        GROUP BY plu_code
    """, (d,))
    pos_rows = cur.fetchall()
    pos_sales = {row["plu_code"]: float(row["qty"] or 0) for row in pos_rows}

    # --- Robobar machine sales (cluster level) ---
    cur.execute("""
        SELECT machine_name, SUM(quantity) as qty
        FROM sales_transactions
        WHERE source = 'Robobar' AND date = %s
        GROUP BY machine_name
    """, (d,))
    machine_rows = cur.fetchall()

    machine_sales = {}
    for row in machine_rows:
        norm_name = normalize_name(row["machine_name"])
        qty = float(row["qty"] or 0)
        machine_sales[norm_name] = machine_sales.get(norm_name, 0) + qty

    # --- Merge by PLU ---
    rows = []
    for plu, mapping in mapping_dict.items():
        pos_qty = pos_sales.get(plu, 0)
        mach_qty = machine_sales.get(mapping["machine_name_norm"], 0)
        variance = pos_qty - mach_qty

        rows.append({
            "plu_code": plu,
            "machine_name": mapping["machine_name_display"],  # pretty
            "pos_sales": round(pos_qty, 2),
            "machine_sales": round(mach_qty, 2),
            "variance": round(variance, 2)
        })

    cur.close()
    conn.close()

    return render_template("variance_robobar.html", rows=rows, selected_date=selected_date)


# ---------------------------
# Vending Mapping
# ---------------------------
@app.route("/mapping/vending", methods=["GET", "POST"])
def mapping_vending():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # List existing mappings
    cur.execute("""
        SELECT id, device_id, slot, plu_code, product_name, store_id, multiplier, is_main, created_at
        FROM vending_mapping
        ORDER BY device_id, slot, plu_code
    """)
    mappings = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("mapping_vending.html", mappings=mappings)


@app.route("/mapping/vending/delete", methods=["POST"])
def delete_mappings_vending():
    ids = request.form.getlist("ids[]")
    if ids:
        conn = get_conn()
        cur = conn.cursor()
        ids = [int(x) for x in ids]
        cur.execute("DELETE FROM vending_mapping WHERE id = ANY(%s)", (ids,))
        conn.commit()
        cur.close()
        conn.close()
        flash(f"❌ Deleted {len(ids)} vending mappings", "warning")
    else:
        flash("⚠️ No mappings selected for deletion", "danger")

    return redirect(url_for("mapping_vending"))


# ---------------------------
# Vending Variance
# ---------------------------
def normalize_plu(plu: str) -> str:
    if not plu:
        return ""
    return re.sub(r"[^a-zA-Z0-9]+", "", plu).upper()

def normalize_name(name: str) -> str:
    if not name:
        return ""
    s = name.lower()
    s = re.sub(r'[\r\n\t]+', ' ', s)
    s = re.sub(r"['`´]", "", s)
    s = re.sub(r'[^a-z0-9&]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


@app.route("/variance/vending", methods=["GET", "POST"])
def variance_vending():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    selected_date = request.form.get("date") or date.today().strftime("%Y-%m-%d")
    d = datetime.strptime(selected_date, "%Y-%m-%d").date()

    # --- Load vending mapping ---
    cur.execute("""
        SELECT device_id, slot, plu_code, product_name, multiplier
        FROM vending_mapping
    """)
    mapping_rows = cur.fetchall()

    # Build lookup
    vending_map = {}
    mapped_plus = set()
    mapped_names = {}
    for row in mapping_rows:
        plu_norm = normalize_plu(row["plu_code"])
        name_norm = normalize_name(row["product_name"])
        key = (str(row["device_id"]), str(row["slot"]))
        vending_map[key] = {
            "plu": plu_norm,
            "name": row["product_name"],
            "multiplier": float(row["multiplier"] or 1)
        }
        mapped_plus.add(plu_norm)
        mapped_names[name_norm] = plu_norm

    # --- POS sales ---
    cur.execute("""
        SELECT plu_code, product_name, SUM(quantity) as qty
        FROM sales_transactions
        WHERE source = 'POS' AND date = %s
        GROUP BY plu_code, product_name
    """, (d,))
    pos_rows = cur.fetchall()

    pos_sales = {}
    for row in pos_rows:
        plu_norm = normalize_plu(row["plu_code"])
        name_norm = normalize_name(row["product_name"])
        qty = float(row["qty"] or 0)

        if plu_norm in mapped_plus:
            key = plu_norm
        elif name_norm in mapped_names:
            key = mapped_names[name_norm]
        else:
            continue  # skip if not mapped

        pos_sales[key] = pos_sales.get(key, 0) + qty

    # --- Vending sales ---
    cur.execute("""
        SELECT device_id, machine_name, SUM(quantity) as qty
        FROM sales_transactions
        WHERE source = 'Vending' AND date = %s
        GROUP BY device_id, machine_name
    """, (d,))
    vending_rows = cur.fetchall()

    machine_sales = {}
    for row in vending_rows:
        key = (str(row["device_id"]), str(row["machine_name"]))
        if key in vending_map:
            plu = vending_map[key]["plu"]
            qty = float(row["qty"] or 0) * vending_map[key]["multiplier"]
            machine_sales[plu] = machine_sales.get(plu, 0) + qty

    cur.close()
    conn.close()

    # --- Build final rows (only mapped PLUs) ---
    rows = []
    for plu in sorted(mapped_plus):
        pos_qty = pos_sales.get(plu, 0.0)
        machine_qty = machine_sales.get(plu, 0.0)
        variance = pos_qty - machine_qty

        # 🔥 Skip products with no activity
        if pos_qty == 0 and machine_qty == 0:
            continue

        product_name = None
        for v in vending_map.values():
            if v["plu"] == plu:
                product_name = v["name"]
                break

        rows.append({
            "plu_code": plu,
            "product_name": product_name or plu,
            "pos_sales": round(pos_qty, 2),
            "machine_sales": round(machine_qty, 2),
            "variance": round(variance, 2)
        })


    return render_template("variance_vending.html", rows=rows, selected_date=selected_date)




if __name__ == "__main__":
    app.run(debug=True)
