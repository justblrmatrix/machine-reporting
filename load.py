import json
import psycopg2
import os

DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def import_nozzle_mapping(json_file="mapping.json"):
    with open(json_file) as f:
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
                    cups = m.get("cups", {})
                    for cup_name, cup in cups.items():
                        if cup.get("base_multiplier") == 1:
                            for mat in cup.get("materials", []):
                                ing = mat["name"].strip()
                                vol = float(mat.get("volume", 0) or 0)

                                cur.execute("""
                                    INSERT INTO nozzle_mapping (store_id, plu_code, ingredient_name, volume)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (store_id, plu_code, ingredient_name) DO UPDATE
                                    SET volume = EXCLUDED.volume
                                """, (store_id, plu, ing, vol))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Imported nozzle mappings.")

if __name__ == "__main__":
    import_nozzle_mapping()
