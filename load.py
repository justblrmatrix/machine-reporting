import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def load_nozzle_mapping():
    with open("mapping.json") as f:
        data = json.load(f)

    conn = get_conn()
    cur = conn.cursor()

    for entry in data:
        meta = entry.get("meta", {})
        store_ids = meta.get("store_ids", [])
        pos_items = entry.get("pos_items", [])
        machine_items = entry.get("machine_items", [])

        for store_id in store_ids:
            for pos in pos_items:
                plu = pos.get("plu_code")
                for m in machine_items:
                    machine_name = m.get("name", "").strip()
                    cups = m.get("cups", {})

                    # Only base multiplier = 1
                    for cup_name, cup in cups.items():
                        if cup.get("base_multiplier", 1) != 1:
                            continue
                        for mat in cup.get("materials", []):
                            ingredient = mat["name"].strip()
                            volume = float(mat.get("volume", 0) or 0)

                            cur.execute("""
                                INSERT INTO nozzle_mapping
                                  (store_id, plu_code, machine_name, ingredient_name, volume)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (store_id, plu, machine_name, ingredient, volume))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Nozzle mapping loaded successfully.")

if __name__ == "__main__":
    load_nozzle_mapping()
