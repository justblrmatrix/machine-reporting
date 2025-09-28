import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DB_URL)

def normalize_plu(plu: str) -> str:
    if not plu:
        return ""
    return plu.replace(" ", "").upper()   # e.g. "JB 3001" → "JB3001"

def load_vending_mapping():
    with open("vending mapping.json") as f:
        data = json.load(f)

    conn = get_conn()
    cur = conn.cursor()

    for block in data:
        machines = block.get("machines", [])
        items = block.get("items", [])

        for item in items:
            name = item.get("name", "").strip()
            plu = normalize_plu(item.get("plucode", ""))
            multiplier = float(item.get("multiplier", 1) or 1)
            is_main = bool(item.get("main", True))
            store_ids = item.get("store_id", [])

            for machine in machines:
                device_id = str(machine["machine_id"])
                for slot in machine.get("slots", []):
                    slot_str = str(slot)

                    for store_id in store_ids:
                        cur.execute("""
                            INSERT INTO vending_mapping
                              (device_id, slot, plu_code, product_name, store_id, multiplier, is_main)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (device_id, slot, plu_code, store_id) DO NOTHING
                        """, (device_id, slot_str, plu, name, store_id, multiplier, is_main))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Vending mapping loaded successfully.")

if __name__ == "__main__":
    load_vending_mapping()
