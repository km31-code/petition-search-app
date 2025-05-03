# db.py
import pandas as pd
from sqlalchemy import create_engine, text

def init_db(db_uri="sqlite:///records.db"):
    """
    Create (or connect to) a SQLite database and ensure
    a 'records' table exists with 'id' and 'line' columns.
    """
    engine = create_engine(db_uri)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                line TEXT
            )
        """))
    return engine

def ingest_csv(engine, csv_file_path):
    """
    Load a CSV file into the 'records' table of our database.
    Each row in the CSV is treated as a single column named 'line'.
    This is a simple approach. For a more complex schema, parse columns accordingly.
    """
    df = pd.read_csv(csv_file_path, header=None, dtype=str, na_filter=False)
    df.columns = ["line"]
    df.to_sql("records", engine, if_exists="append", index=False)

def search_records(engine, query_str):
    """
    Perform a partial-match search on the 'line' column of our 'records' table
    using a SQL LIKE query. Returns a list of matching strings.
    """
    wildcard = f"%{query_str}%"
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT line FROM records WHERE line LIKE :wildcard"),
            {"wildcard": wildcard},
        )
        return [row[0] for row in result]
