import csv
import psycopg2
from openpyxl import load_workbook

from dotenv import load_dotenv
import os

# load env
load_dotenv(r"C:\Users\ASUS\Desktop\imei-pipeline\.env")


password = os.getenv("password")

if not password:
    raise Exception("Password not found in .env")

excel_file = r"C:\IMPORTSERVER\IMEI\IMEI.xlsx"
csv_file = r"C:\IMPORTSERVER\IMEI\temp.csv"

# DB config
db_config = {
    "host": "localhost",
    "database": "imei_db",
    "user": "postgres",
    "password":password,
    "port": 5432
}
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

wb = load_workbook(excel_file, read_only=True, data_only=True)
ws = wb.active

rows = ws.iter_rows(values_only=True)

headers = next(rows)

def clean(col):
    return str(col).strip().replace(" ", "_").replace("-", "_").lower()

columns = [clean(col) for col in headers]

table_name = "imei_data"

cur.execute(f"DROP TABLE IF EXISTS {table_name};")

create_query = f"""
CREATE TABLE {table_name} (
    {', '.join([f'"{col}" TEXT' for col in columns])}
);
"""
cur.execute(create_query)

with open(csv_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(columns)

    for row in rows:
        writer.writerow([str(cell) if cell is not None else "" for cell in row])

with open(csv_file, "r", encoding="utf-8") as f:
    cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

cur.execute("""
ALTER TABLE imei_data
ADD COLUMN act_date TEXT,
ADD COLUMN act_day TEXT;
""")

cur.execute("""
UPDATE imei_data
SET 
    act_date = TO_CHAR(activation_time::timestamp, 'DD-MM-YYYY'),
    act_day  = TRIM(TO_CHAR(activation_time::timestamp, 'Day'))
WHERE activation_time ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}';
""")

conn.commit()
cur.close()
conn.close()

print("DONE")