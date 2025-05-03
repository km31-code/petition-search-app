# app.py
import streamlit as st
import tempfile
import os
import pandas as pd
import re
import json
from sqlalchemy.sql import text
from db import init_db, ingest_csv_in_chunks, ingest_txt_in_chunks, search_records, search_records_by_columns, get_csv_preview, ingest_csv_with_selected_columns


def try_parse_into_columns(text_data, delimiters=['\t', ' ', ',']):
    """
    Attempts to convert a plain text line into columns based on detected delimiters.
    Returns a DataFrame where each column has been identified.
    """
    # If text_data is a Series or list of strings
    if isinstance(text_data, (pd.Series, list)):
        # Parse the first few rows to determine column structure
        sample_rows = text_data[:5] if len(text_data) > 5 else text_data
        
        # Try each delimiter and choose the one that produces the most consistent columns
        best_delimiter = ' '  # Default delimiter
        most_consistent_count = 0
        most_columns = 0
        
        for delimiter in delimiters:
            # Try to split each row by the delimiter and count fields
            field_counts = [len(row.split(delimiter)) for row in sample_rows if isinstance(row, str)]
            
            if not field_counts:
                continue
                
            # Count how many rows have the most common field count
            if field_counts:
                most_common_count = max(set(field_counts), key=field_counts.count)
                consistency = field_counts.count(most_common_count) / len(field_counts) if field_counts else 0
                
                # Choose this delimiter if it gives more consistent columns or more columns with the same consistency
                if (consistency > most_consistent_count or 
                    (consistency == most_consistent_count and most_common_count > most_columns)):
                    most_consistent_count = consistency
                    most_columns = most_common_count
                    best_delimiter = delimiter
        
        # Now parse all rows with the chosen delimiter
        parsed_rows = []
        max_columns = 0
        
        for row in text_data:
            if not isinstance(row, str):
                continue
                
            fields = row.split(best_delimiter)
            max_columns = max(max_columns, len(fields))
            parsed_rows.append(fields)
        
        # Ensure all rows have the same number of columns
        for i in range(len(parsed_rows)):
            if len(parsed_rows[i]) < max_columns:
                parsed_rows[i].extend([''] * (max_columns - len(parsed_rows[i])))
        
        # Generate column names if we have data
        if parsed_rows and max_columns > 0:
            column_names = [f'Column {i+1}' for i in range(max_columns)]
            return pd.DataFrame(parsed_rows, columns=column_names)
    
    # Fallback: return original data as a single column
    if isinstance(text_data, pd.DataFrame):
        return text_data
    else:
        return pd.DataFrame(text_data, columns=['Content'])


def format_results_as_table(results, selected_columns=None):
    """
    Format search results into a DataFrame with proper columns.
    If column_data is available and selected_columns is provided, 
    will extract those specific columns.
    """
    if not results:
        return pd.DataFrame()
    
    # Check if we have structured column data (from the new format)
    has_column_data = isinstance(results[0], tuple) and len(results[0]) > 1 and results[0][1]
    
    if has_column_data and selected_columns:
        # Extract structured data from JSON column_data
        rows = []
        for row_text, column_json in results:
            try:
                # Parse the JSON column data
                if isinstance(column_json, str):
                    column_data = json.loads(column_json)
                else:
                    column_data = column_json
                
                # Create a row with only the selected columns
                row_data = {}
                for col in selected_columns:
                    col_key = f"Column {col}"
                    if col_key in column_data:
                        row_data[col_key] = column_data[col_key]
                rows.append(row_data)
            except Exception as e:
                # If JSON parsing fails, use the raw text
                rows.append({"Content": row_text})
        
        # Create DataFrame from the structured data
        if rows:
            df = pd.DataFrame(rows)
            # Use more user-friendly column names if they were provided
            return df
    
    # Fallback to the old method if we don't have structured data
    if has_column_data:
        # Just use the line text from the tuple
        text_data = [row[0] for row in results]
    else:
        text_data = results
    
    # Try to parse into columns using delimiters
    return try_parse_into_columns(text_data, delimiters=['\t', ' ', ','])


def main():
    st.title("Petition Search App")
    st.markdown("""
    <style>
    .stDataFrame {
        width: 100%;
    }
    .dataframe th {
        background-color: #f0f2f6;
        font-weight: bold;
        text-align: left !important;
    }
    .dataframe td {
        text-align: left !important;
    }
    .st-emotion-cache-1dm5gw7 {
        max-width: 100%;
        overflow-x: auto;
    }
    .column-selector {
        padding: 10px;
        margin: 5px;
        border-radius: 5px;
        text-align: center;
    }
    .selected {
        background-color: #f63366;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)

    # Initialize session state for tracking
    if 'file_uploaded' not in st.session_state:
        st.session_state.file_uploaded = False
    if 'file_previewed' not in st.session_state:
        st.session_state.file_previewed = False
    if 'file_ingested' not in st.session_state:
        st.session_state.file_ingested = False
    if 'ingestion_complete' not in st.session_state:
        st.session_state.ingestion_complete = False
    if 'preview_data' not in st.session_state:
        st.session_state.preview_data = None
    if 'selected_columns' not in st.session_state:
        st.session_state.selected_columns = []
    if 'detected_delimiter' not in st.session_state:
        st.session_state.detected_delimiter = None
    if 'temp_filepath' not in st.session_state:
        st.session_state.temp_filepath = None
    if 'file_type' not in st.session_state:
        st.session_state.file_type = None

    st.write("""
        This app can handle large CSV/TXT files in chunks.
        After uploading, you'll see a preview of the file and can select specific columns to search.
    """)

    # 1) Initialize or connect to the DB
    engine = init_db()

    # 2) File Uploader (CSV or TXT)
    uploaded_file = st.file_uploader("Upload your CSV or TXT file", type=["csv", "txt"], 
                                    key="file_uploader")
    
    # File upload section
    col1, col2 = st.columns([3, 1])
    with col1:
        preview_button = st.button("Preview File", disabled=(uploaded_file is None))
    with col2:
        clear_db = st.checkbox("Clear existing data", value=False)
    
    # If a file is uploaded and the preview button is clicked
    if preview_button and uploaded_file is not None:
        st.session_state.file_uploaded = True
        st.session_state.file_previewed = False
        st.session_state.file_ingested = False
        st.session_state.ingestion_complete = False
        st.session_state.selected_columns = []
        
        # Clear existing records if requested
        if clear_db:
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM records"))
                st.success("Previous data cleared.")

        # Write uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(uploaded_file.read())
            tmp.flush()
            st.session_state.temp_filepath = tmp.name

        # Get file type
        _, file_extension = os.path.splitext(uploaded_file.name)
        st.session_state.file_type = file_extension.lower()
        
        # For CSV files, show a preview and column selection
        if st.session_state.file_type == ".csv":
            with st.spinner("Generating preview..."):
                preview_df, detected_delimiter = get_csv_preview(st.session_state.temp_filepath)
                st.session_state.preview_data = preview_df
                st.session_state.detected_delimiter = detected_delimiter
                st.session_state.file_previewed = True
        # For TXT files, just preview without column selection
        elif st.session_state.file_type == ".txt":
            with st.spinner("Generating preview..."):
                # For text files, we just show a simple preview
                with open(st.session_state.temp_filepath, 'r', encoding='utf-8', errors='replace') as f:
                    preview_lines = [f.readline().strip() for _ in range(50) if f.readline()]
                preview_df = pd.DataFrame({"Content": preview_lines})
                st.session_state.preview_data = preview_df
                st.session_state.file_previewed = True
        else:
            st.error("Unsupported file type. Please upload a .csv or .txt file.")
    
    # Display file preview and column selection for CSV files
    if st.session_state.file_previewed and st.session_state.preview_data is not None:
        st.subheader("File Preview")
        
        # Display the preview as a formatted table
        st.dataframe(st.session_state.preview_data, use_container_width=True)
        
        # Column selection for CSV files
        if st.session_state.file_type == ".csv":
            st.subheader("Select Columns to Search")
            st.write("Select the columns you want to include in your search. This will make searching faster and results more focused.")
            
            # Show column selection with better UI
            columns = st.session_state.preview_data.columns
            
            # Filter out columns that are completely empty in the preview
            non_empty_columns = []
            for col in columns:
                # Check if the column has any non-empty values
                if st.session_state.preview_data[col].astype(str).str.strip().str.len().sum() > 0:
                    non_empty_columns.append(col)
            
            if len(non_empty_columns) == 0:
                st.warning("No columns with data were found in the preview.")
            else:
                # Create a more structured checklist layout
                st.write("Select which columns to include:")
                
                # Create a container with columns for the selection UI
                selection_container = st.container()
                
                # Calculate a reasonable number of columns based on screen size
                cols_per_row = 3
                
                # Group columns for layout
                col_groups = [non_empty_columns[i:i + cols_per_row] for i in range(0, len(non_empty_columns), cols_per_row)]
                
                with selection_container:
                    # Use expanders to group similar columns
                    # First try to identify if there are patterns in column data to group them
                    
                    # For simplicity, group by 10 columns each
                    group_size = 10
                    column_groups = [non_empty_columns[i:i+group_size] for i in range(0, len(non_empty_columns), group_size)]
                    
                    for group_idx, column_group in enumerate(column_groups):
                        # Try to identify a common theme in the group for the expander title
                        # For now, just use a generic group name
                        group_label = f"Column Group {group_idx+1} (Columns {group_idx*group_size} - {min((group_idx+1)*group_size-1, len(non_empty_columns)-1)})"
                        
                        with st.expander(group_label, expanded=(group_idx == 0)):
                            for col in column_group:
                                # Extract column index
                                col_idx = int(col.split()[1]) if len(col.split()) > 1 and col.split()[1].isdigit() else 0
                                
                                # Show a sample value from the column
                                sample_values = st.session_state.preview_data[col].astype(str).str.strip()
                                sample_values = sample_values[sample_values != ""]
                                sample_value = sample_values.iloc[0] if len(sample_values) > 0 else ""
                                
                                # Create a more readable display name
                                if sample_value:
                                    display_name = f"{col} (Sample: {sample_value[:30]}{'...' if len(sample_value) > 30 else ''})"
                                else:
                                    display_name = col
                                
                                # Use checkboxes for selection
                                if st.checkbox(
                                    display_name,
                                    value=(col in st.session_state.selected_columns),
                                    key=f"col_{col_idx}"
                                ):
                                    if col not in st.session_state.selected_columns:
                                        st.session_state.selected_columns.append(col)
                                else:
                                    if col in st.session_state.selected_columns:
                                        st.session_state.selected_columns.remove(col)
                
                # Add "Select All" and "Clear All" buttons
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Select All", key="select_all"):
                        st.session_state.selected_columns = non_empty_columns.copy()
                        st.rerun()
                with col2:
                    if st.button("Clear All", key="clear_all"):
                        st.session_state.selected_columns = []
                        st.rerun()
                
                # Display a summary of selected columns
                if st.session_state.selected_columns:
                    st.write(f"Selected {len(st.session_state.selected_columns)} columns")
                    # Show the selected columns in a readable format
                    selected_cols_df = pd.DataFrame({
                        "Selected Columns": sorted(st.session_state.selected_columns, 
                                                key=lambda x: int(x.split()[1]) if len(x.split()) > 1 and x.split()[1].isdigit() else 0)
                    })
                    st.dataframe(selected_cols_df, height=150)
                else:
                    st.warning("Please select at least one column to include in the search.")
        
        # Ingest button
        ingest_button = st.button(
            "Process and Ingest File", 
            disabled=(st.session_state.file_type == ".csv" and not st.session_state.selected_columns)
        )
        
        if ingest_button:
            st.session_state.file_ingested = True
            
            with st.spinner("Processing file..."):
                if st.session_state.file_type == ".csv":
                    # Get column indices from names
                    column_indices = [int(col.split()[1]) for col in st.session_state.selected_columns if len(col.split()) > 1 and col.split()[1].isdigit()]
                    
                    # Ingest with only selected columns
                    preview_data = ingest_csv_with_selected_columns(
                        engine, 
                        st.session_state.temp_filepath,
                        st.session_state.selected_columns,
                        st.session_state.detected_delimiter
                    )
                elif st.session_state.file_type == ".txt":
                    preview_data = ingest_txt_in_chunks(engine, st.session_state.temp_filepath)
            
            st.session_state.ingestion_complete = True
            st.success("File processed and ingested successfully!")

    # Search section - only show if ingestion is complete
    if st.session_state.ingestion_complete:
        st.subheader("Search the Database")

        # Multi-token partial substring search
        query_str = st.text_input("Enter search terms:")

        # Add slider for result limit
        result_limit = st.slider("Maximum results to show:", 10, 2000, 1000, 100)

        if st.button("Search", key="search_button"):
            if query_str.strip():
                with st.spinner("Searching..."):
                    # If we have selected columns for a CSV file, use the column-specific search
                    if st.session_state.file_type == ".csv" and st.session_state.selected_columns:
                        # Get column indices from names
                        column_indices = [int(col.split()[1]) for col in st.session_state.selected_columns if len(col.split()) > 1 and col.split()[1].isdigit()]
                        results = search_records_by_columns(engine, query_str.strip(), column_indices, limit=result_limit)
                    else:
                        # Use the regular search for TXT files or if no columns were selected
                        results = search_records(engine, query_str.strip(), limit=result_limit)
                
                st.write(f"Found {len(results)} result(s).")

                # If results exist, display them in a clean format
                if results:
                    # Format results based on selected columns
                    if st.session_state.file_type == ".csv" and st.session_state.selected_columns:
                        column_indices = [int(col.split()[1]) for col in st.session_state.selected_columns if len(col.split()) > 1 and col.split()[1].isdigit()]
                        results_df = format_results_as_table(results, column_indices)
                    else:
                        results_df = format_results_as_table(results)
                    
                    # Limit columns for display if there are too many
                    max_cols_to_display = 10
                    if len(results_df.columns) > max_cols_to_display:
                        display_cols = results_df.columns[:max_cols_to_display]
                        results_df_display = results_df[display_cols].copy()
                        # Add an indicator column
                        results_df_display['...'] = '...'
                    else:
                        results_df_display = results_df.copy()
                    
                    # Apply highlighting to search terms with a more subtle style
                    def highlight_terms(text, terms):
                        if pd.isna(text):
                            return text
                        text_str = str(text)
                        for term in terms:
                            if term.lower() in text_str.lower():
                                pattern = re.compile(f'({re.escape(term)})', re.IGNORECASE)
                                text_str = pattern.sub(r'<span style="border-bottom: 1px solid #0066cc; color: #0066cc; font-weight: 500;">\1</span>', text_str)
                        return text_str
                    
                    # Apply highlighting to search results
                    styled_df = results_df_display.copy()
                    search_terms = [term.strip() for term in query_str.split() if term.strip()]
                    
                    for col in styled_df.columns:
                        styled_df[col] = styled_df[col].apply(lambda x: highlight_terms(x, search_terms))
                    
                    # Display as HTML table to preserve highlighting
                    st.markdown(
                        styled_df.to_html(escape=False, index=False),
                        unsafe_allow_html=True
                    )

                    # Option to download results
                    csv = results_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Results as CSV",
                        data=csv,
                        file_name="search_results.csv",
                        mime="text/csv",
                    )
                else:
                    st.write("No results found.")
            else:
                st.warning("Please enter a valid search term.")


if __name__ == "__main__":
    main()
