import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from pathlib import Path

# Load env vars
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
db_url = os.getenv("SUPABASE_DB_URL")

# Excel -> Table mapping
SHEETS_AND_TABLES = {
    "Cohort 3 DS": "raw_datascience_intake",
    "Cohort 3 DA": "raw_dataanalyst_intake"
}

# Define column order (must match DB schema)
COLUMNS = [
    "timestamp",
    "id_no",
    "age_range",
    "gender",
    "country",
    "referral_source",
    "years_experience",
    "track",
    "weekly_commitment",
    "main_aim",
    "motivation",
    "skill_level",
    "aptitude_test_completed",
    "total_score",
    "graduated"
]

COLUMN_MAP = {
    "Timestamp": "timestamp",
    "Id. No": "id_no",
    "Age range": "age_range",
    "Gender": "gender",
    "Country": "country",
    "Where did you hear about Everything Data?": "referral_source",
    "How many years of learning experience do you have in the field of data?": "years_experience",
    "Which track are you applying for?": "track",
    "How many hours per week can you commit to learning?": "weekly_commitment",
    "What is your main aim for joining the mentorship program?": "main_aim",
    "What is your motivation to join the Everything Data mentorship program?": "motivation",
    "How best would you describe your skill level in the track you are applying for?": "skill_level",
    "Have you completed the everything data aptitude test for your track?": "aptitude_test_completed",
    "Total score": "total_score",
    "Graduated": "graduated"
}


CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS {table_name};
CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMPTZ,
    id_no TEXT PRIMARY KEY,
    age_range TEXT,
    gender TEXT,
    country TEXT,
    referral_source TEXT,
    years_experience TEXT,
    track TEXT,
    weekly_commitment TEXT,
    main_aim TEXT,
    motivation TEXT,
    skill_level TEXT,
    aptitude_test_completed TEXT,
    total_score FLOAT,
    graduated BOOLEAN
);
"""

def ensure_table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL.format(table_name=table_name))
    conn.commit()
    print(f"table {table_name} ready")

def upsert_excel_sheet_to_table(file_path, sheet_name, table_name, conn):
    df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Rename columns explicitly to match DB
    df = df.rename(columns=COLUMN_MAP)

    # Keep only expected columns and align order
    df = df[[col for col in COLUMN_MAP.values() if col in df.columns]]
    df = df.reindex(columns=COLUMNS)

    # Drop rows missing id_no 
    df = df.dropna(subset=["id_no"])


    rows = df.to_dict(orient="records")

    with conn.cursor() as cur:
        query = f"""
        INSERT INTO {table_name} ({",".join(COLUMNS)})
        VALUES ({",".join(["%s"] * len(COLUMNS))})
        ON CONFLICT (id_no) DO UPDATE SET
          {",".join([f"{col}=EXCLUDED.{col}" for col in COLUMNS if col != "id_no"])};
        """

        values = [[row.get(col) for col in COLUMNS] for row in rows]
        execute_batch(cur, query, values, page_size=100)

    conn.commit()
    print(f"Upserted {len(rows)} rows into {table_name}")

def main():
    conn = psycopg2.connect(db_url)
    excel_file = "data/Cohort_4_Capstone_Dataset.xlsx"

    for sheet, table_name in SHEETS_AND_TABLES.items():
        ensure_table_exists(conn, table_name)
        upsert_excel_sheet_to_table(excel_file, sheet, table_name, conn)

    conn.close()

if __name__ == "__main__":
    main()
