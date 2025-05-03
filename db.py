# db.py
import pandas as pd
from sqlalchemy import create_engine, text, event
import os
import json
import time
import sqlite3

def init_db(db_uri="sqlite:///:memory:"):
    """
    Create an in-memory SQLite database and ensure a 'records' table exists.
    This avoids creating large files that won't sync with GitHub.
    """
    # Create the engine with conservative connection settings
    engine = create_engine(
        db_uri,
        connect_args={
            "timeout": 30.0,  # Wait up to 30 seconds for locks to clear
            "isolation_level": None,  # Autocommit mode
            "check_same_thread": False,  # Allow cross-thread connections
        }
    )
    
    # Create the table structure
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                line TEXT,
                column_data TEXT
            )
        """))
        
        # Add simple index for faster searches
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_line ON records(line)"))
    
    return engine

def ingest_csv_in_chunks(engine, csv_path, chunksize=10000):
    """
    Ingest a large CSV or space-delimited file into the 'records' table in chunks.
    Each row is stored as a single line (TEXT).
    """
    # We'll store the first chunk's data in memory to display a small preview
    first_chunk_data = None

    # First check if the file is empty
    try:
        file_size = os.path.getsize(csv_path)
        if (file_size == 0):
            return pd.DataFrame({"line": ["File is empty"]})
    except Exception as e:
        return pd.DataFrame({"line": [f"Error checking file: {str(e)}"]})

    # Try different approaches to read the file
    for delimiter in ['\t', ',', ' ']:
        try:
            # Try reading with this delimiter
            reader = pd.read_csv(
                csv_path,
                sep=delimiter,  
                header=None,
                dtype=str,
                na_filter=False,
                engine='python',
                on_bad_lines='skip',
                chunksize=chunksize,
                quoting=3,  # QUOTE_NONE - don't use quotes for quoting
                encoding_errors='replace'  # Replace encoding errors
            )
            
            # Process the file in chunks
            for i, chunk in enumerate(reader):
                # If the DataFrame has multiple columns, convert to a single line
                if len(chunk.columns) > 1:
                    # Join all columns with a space to create a single line
                    chunk['line'] = chunk.apply(lambda row: ' '.join(row.astype(str).values), axis=1)
                    chunk['column_data'] = chunk.apply(lambda row: row.to_json(), axis=1)
                    chunk = chunk[['line', 'column_data']]  # Keep only the joined column and JSON data
                else:
                    chunk.columns = ["line"]  # Single column called 'line'
                    chunk['column_data'] = chunk.apply(lambda row: row.to_json(), axis=1)
                
                if i == 0:
                    first_chunk_data = chunk.head(50)  # keep up to 50 lines for preview
                
                # Append chunk to DB
                chunk.to_sql("records", engine, if_exists="append", index=False)
            
            # If we got here, we successfully processed the file
            return first_chunk_data
        
        except Exception as e:
            # Log the exception but continue trying other delimiters
            print(f"Failed with delimiter '{delimiter}': {str(e)}")
            continue
    
    # If all approaches failed, try a more robust approach
    try:
        lines = []
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
            count = 0
            for line in f:
                if count < 50:
                    lines.append(line.strip())
                count += 1
                if count % chunksize == 0:
                    chunk_df = pd.DataFrame({"line": lines})
                    chunk_df.to_sql("records", engine, if_exists="append", index=False)
                    if count <= chunksize:
                        first_chunk_data = chunk_df.head(50)
                    lines = []
            
            # Handle any remaining lines
            if lines:
                chunk_df = pd.DataFrame({"line": lines})
                chunk_df.to_sql("records", engine, if_exists="append", index=False)
                if first_chunk_data is None:
                    first_chunk_data = chunk_df.head(50)
        
        return first_chunk_data
    
    except Exception as e:
        # Last resort: return an error message
        return pd.DataFrame({"line": [f"Failed to process file: {str(e)}"]})

def ingest_txt_in_chunks(engine, txt_path, chunksize=50000):
    """
    Ingest a large text file into the 'records' table in chunks of lines.
    """
    first_chunk_data = []
    buffer = []
    count = 0
    chunk_index = 0

    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            buffer.append(line.strip("\n\r"))
            count += 1

            # If we've reached the chunksize, store this chunk in the DB
            if count % chunksize == 0:
                chunk_df = pd.DataFrame({"line": buffer})
                # Save chunk to DB
                chunk_df.to_sql("records", engine, if_exists="append", index=False)
                # Save chunk #0 for preview
                if chunk_index == 0:
                    first_chunk_data = chunk_df.head(50)
                buffer = []
                chunk_index += 1

    # If there are leftover lines in the buffer after the loop ends
    if buffer:
        chunk_df = pd.DataFrame({"line": buffer})
        chunk_df.to_sql("records", engine, if_exists="append", index=False)
        if chunk_index == 0:  # means everything fit into one chunk
            first_chunk_data = chunk_df.head(50)

    return first_chunk_data

def search_records(engine, query_str, limit=1000):
    """
    Perform a multi-token, case-insensitive, partial-substring search.
    Example: "cas ford lie" => must find "cas" AND "ford" AND "lie" in the line.
    Uses LIMIT for faster results and memory efficiency.
    """
    # Split user query into tokens (strip whitespace, lowercase, etc.)
    tokens = [t.strip().lower() for t in query_str.split() if t.strip()]
    if not tokens:
        return []

    # Build a WHERE clause with AND across each token:
    conditions = []
    params = {}
    for i, token in enumerate(tokens):
        param_name = f"token{i}"
        conditions.append(f"lower(line) LIKE :{param_name}")
        # wrap each token in %...% for partial substring matching
        params[param_name] = f"%{token}%"

    where_clause = " AND ".join(conditions)
    # Add LIMIT clause to return faster
    query = f"SELECT line FROM records WHERE {where_clause} LIMIT {limit}"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return [row[0] for row in result]

def get_csv_preview(filepath, rows=50, delimiters=['\t', ',', ' ']):
    """
    Get a preview of the CSV file and detect the delimiter and columns.
    Returns a tuple of (dataframe, detected_delimiter)
    """
    # First check if the file is empty
    try:
        file_size = os.path.getsize(filepath)
        if file_size == 0:
            return pd.DataFrame({"Content": ["File is empty"]}), None
    except Exception as e:
        return pd.DataFrame({"Error": [f"Error checking file: {str(e)}"]}), None

    # Try different delimiters and choose the best one
    best_delimiter = None
    best_df = None
    max_columns = 0

    for delimiter in delimiters:
        try:
            # Try reading with this delimiter
            df = pd.read_csv(
                filepath,
                sep=delimiter,
                header=None,
                dtype=str,
                na_filter=False,
                engine='python',
                nrows=rows,
                on_bad_lines='skip',
                quoting=3  # QUOTE_NONE
            )
            
            # If this delimiter gives more columns, it's likely better
            if len(df.columns) > max_columns:
                max_columns = len(df.columns)
                best_delimiter = delimiter
                best_df = df
                
        except Exception as e:
            print(f"Failed to read with delimiter '{delimiter}': {str(e)}")
            continue
    
    # If we didn't find a good delimiter, try a more basic approach
    if best_df is None:
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = [f.readline().strip() for _ in range(rows) if f.readline()]
                best_df = pd.DataFrame({"Content": lines})
                best_delimiter = None
        except Exception as e:
            return pd.DataFrame({"Error": [f"Failed to read file: {str(e)}"]}), None
    
    # Generate column names if we have data
    if best_df is not None and len(best_df.columns) > 0:
        best_df.columns = [f'Column {i}' for i in range(len(best_df.columns))]
    
    return best_df, best_delimiter

def ingest_csv_with_selected_columns(engine, csv_path, selected_columns, delimiter, chunksize=50000):
    """
    Streamlined CSV ingestion that only processes selected columns.
    Uses direct batch SQL insertions for maximum performance.
    """
    try:
        # Extract column indices from names
        column_indices = [int(col.split()[1]) for col in selected_columns if len(col.split()) > 1 and col.split()[1].isdigit()]
        if not column_indices:
            return pd.DataFrame({"line": ["No valid columns selected"]})
            
        # Define a function to efficiently process a chunk
        def process_chunk(df, column_indices):
            # Only keep the columns we need
            cols_to_use = min(len(df.columns), max(column_indices) + 1)
            
            # Only extract the columns we want
            result_df = pd.DataFrame()
            
            # Create the text representation for combined line search
            selected_cols = [df.iloc[:, idx] if idx < len(df.columns) else pd.Series([''] * len(df)) for idx in column_indices]
            result_df['line'] = pd.concat(selected_cols, axis=1).apply(lambda row: ' '.join(row.astype(str).str.strip()), axis=1)
            
            # Create a simplified column_data field without overhead of full JSON
            column_data_list = []
            for _, row in pd.concat(selected_cols, axis=1).iterrows():
                col_data = {}
                for idx, col_idx in enumerate(column_indices):
                    if idx < len(row):
                        col_data[f"Column {col_idx}"] = str(row.iloc[idx]).strip()
                column_data_list.append(json.dumps(col_data))
            
            result_df['column_data'] = column_data_list
            return result_df
        
        # Try to open with very efficient parameters
        try:
            # Only read the specific columns we need instead of the whole file
            # Skip header inference and other slow operations
            reader = pd.read_csv(
                csv_path,
                sep=delimiter,
                header=None,
                usecols=range(max(column_indices) + 1),  # Only read up to the max column we need
                dtype=str,
                na_filter=False,
                engine='c',  # Use the faster C engine
                on_bad_lines='skip',
                chunksize=chunksize,
                quoting=3,  # QUOTE_NONE
                low_memory=True,
                memory_map=True
            )
        except:
            # Fallback to the more flexible but slower python engine if needed
            reader = pd.read_csv(
                csv_path,
                sep=delimiter,
                header=None,
                usecols=range(max(column_indices) + 1),  # Only read columns up to the max we need
                dtype=str,
                na_filter=False,
                engine='python',
                on_bad_lines='skip',
                chunksize=chunksize,
                quoting=3  # QUOTE_NONE
            )
        
        # Create a connection for direct SQL inserts
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        # Prepare the database - create a temporary table for faster bulk inserts
        # This will avoid index updates during insertion
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute("CREATE TEMPORARY TABLE temp_records (line TEXT, column_data TEXT)")
        
        first_chunk_data = None
        rows_processed = 0
        
        # Process each chunk and insert directly
        for i, chunk in enumerate(reader):
            # Process and keep only what we need
            processed_df = process_chunk(chunk, column_indices)
            
            if i == 0:
                first_chunk_data = processed_df.head(50)
                
            # Use efficient bulk inserts
            records = list(zip(processed_df['line'], processed_df['column_data']))
            cursor.executemany("INSERT INTO temp_records VALUES (?, ?)", records)
            
            rows_processed += len(records)
            
            # Commit every few chunks to avoid transaction issues
            if i % 10 == 0:
                conn.commit()
                cursor.execute("BEGIN TRANSACTION")
                
        # Move data from temp table to main table
        cursor.execute("INSERT INTO records (line, column_data) SELECT line, column_data FROM temp_records")
        cursor.execute("DROP TABLE temp_records")
        
        # Commit and clean up
        conn.commit()
        cursor.close()
        conn.close()
        
        return first_chunk_data
        
    except Exception as e:
        print(f"Error ingesting CSV with selected columns: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame({"line": [f"Error ingesting file: {str(e)}"]})

def search_records_by_columns(engine, query_str, selected_columns=None, limit=1000):
    """
    Search records but only within the selected columns if provided.
    """
    # Split user query into tokens
    tokens = [t.strip().lower() for t in query_str.split() if t.strip()]
    if not tokens:
        return []
    
    # Build the query differently based on whether we're searching specific columns
    conditions = []
    params = {}
    
    if selected_columns and len(selected_columns) > 0:
        # For each token, search across all selected columns
        for i, token in enumerate(tokens):
            column_conditions = []
            param_name = f"token{i}"
            params[param_name] = f"%{token}%"
            
            # SQLite doesn't have JSON_EXTRACT by default, so fall back to searching the whole line
            # We'll filter the results when displaying
            column_conditions.append(f"lower(line) LIKE :{param_name}")
            
            # Combine with OR for the same token across different columns
            if column_conditions:
                conditions.append(f"({' OR '.join(column_conditions)})")
    else:
        # If no columns specified, search the entire line
        for i, token in enumerate(tokens):
            param_name = f"token{i}"
            conditions.append(f"lower(line) LIKE :{param_name}")
            params[param_name] = f"%{token}%"
    
    # Combine all token conditions with AND
    where_clause = " AND ".join(conditions)
    
    # Build and execute the query
    query = f"""
        SELECT line, column_data FROM records 
        WHERE {where_clause} 
        LIMIT {limit}
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        return [(row[0], row[1]) for row in result]
