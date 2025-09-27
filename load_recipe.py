import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def load_cocktail_recipes():
    with open("mapping.json") as f:
        data = json.load(f)

    conn = get_conn()
    cur = conn.cursor()

    inserted = 0

    for entry in data:
        meta = entry.get("meta", {})
        store_ids = meta.get("store_ids", [])
        pos_items = entry.get("pos_items", [])
        machine_items = entry.get("machine_items", [])

        # If multiple machine materials are listed → it's a cocktail
        for store_id in store_ids:
            for pos in pos_items:
                plu = pos.get("plu_code")
                if not plu:
                    continue

                # Build recipe from all machine cups + materials
                for m in machine_items:
                    cups = m.get("cups", {})
                    for cup_name, cup in cups.items():
                        if cup.get("base_multiplier", 1) != 1:
                            continue
                        materials = cup.get("materials", [])
                        if len(materials) <= 1:
                            # Skip pure ingredient cases (handled in nozzle_mapping)
                            continue

                        for mat in materials:
                            ingredient = mat["name"].strip()
                            volume = float(mat.get("volume", 0) or 0)

                            cur.execute("""
                                INSERT INTO cocktail_recipes (store_id, cocktail_plu, ingredient_name, volume_ml)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (store_id, cocktail_plu, ingredient_name) DO NOTHING
                            """, (store_id, plu, ingredient, volume))
                            inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Cocktail recipes loaded successfully. Inserted {inserted} rows.")

if __name__ == "__main__":
    load_cocktail_recipes()
