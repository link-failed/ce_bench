import json
import pandas as pd
import sqlglot
from sqlglot import parse_one
from sqlglot.expressions import Table, Column, Identifier
from typing import Dict, Set, Tuple, Optional, List


class SQLMapper:
    def __init__(self, table_mapping: Dict[str, str], column_mapping: Dict[str, str]):
        self.table_mapping = {k.upper(): v for k, v in table_mapping.items()}
        self.column_mapping = {k.upper(): v for k, v in column_mapping.items()}
    
    def extract_tables_and_columns(self, sql_query: str) -> Tuple[Set[str], Set[str]]:
        try:
            parsed = parse_one(sql_query, dialect="sqlite")
            
            tables = set()
            columns = set()
            
            for table in parsed.find_all(Table):
                tables.add(table.name.upper())
            
            for column in parsed.find_all(Column):
                columns.add(column.name.upper())
            
            return tables, columns
            
        except Exception:
            return set(), set()
    
    def map_sql_query(self, sql_query: str, pretty: bool = True) -> Optional[str]:
        try:
            parsed = parse_one(sql_query, dialect="sqlite")
            
            def transform_table(node):
                if isinstance(node, Table):
                    original_name = node.name.upper()
                    if original_name in self.table_mapping:
                        node.set("this", Identifier(this=self.table_mapping[original_name]))
                return node
            
            def transform_column(node):
                if isinstance(node, Column):
                    # Handle table qualification (e.g., table.column)
                    if node.table:
                        original_table_name = node.table.upper()
                        if original_table_name in self.table_mapping:
                            node.set("table", Identifier(this=self.table_mapping[original_table_name]))
                    
                    # Handle column name mapping
                    original_name = node.name.upper()
                    if original_name in self.column_mapping:
                        node.set("this", Identifier(this=self.column_mapping[original_name]))
                return node
            
            transformed = parsed.transform(transform_table)
            transformed = transformed.transform(transform_column)
            
            return transformed.sql(dialect="sqlite", pretty=pretty)
            
        except Exception:
            return None
    
def load_mapping_from_schemas_json(file_path: str, database_name: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        if database_name not in data:
            raise ValueError(f"Database '{database_name}' not found in schemas file")
        
        mapping = data[database_name]["mapping"]
        return mapping["tables"], mapping["columns"]
        
    except Exception:
        return {}, {}


def process_bird_csv(csv_path: str, schemas_path: str, output_path: str = None):
    """Process all q1 and q2 queries in bird.csv using the SQL mapper."""
    
    # Load the CSV file
    df = pd.read_csv(csv_path)
    
    # Load all schemas data once
    with open(schemas_path, 'r') as f:
        schemas_data = json.load(f)
    
    # Add new columns for mapped queries if they don't exist
    if 'q1_mapped' not in df.columns:
        df['q1_mapped'] = None
    if 'q2_mapped' not in df.columns:
        df['q2_mapped'] = None
    
    for idx, row in df.iterrows():
        dbid = row['dbid']
        q1 = row['q1'] if pd.notna(row['q1']) else None
        q2 = row['q2'] if pd.notna(row['q2']) else None
            
        # Get mappings for this database
        if dbid in schemas_data:
            table_mapping = schemas_data[dbid]["mapping"]["tables"]
            column_mapping = schemas_data[dbid]["mapping"]["columns"]
            mapper = SQLMapper(table_mapping, column_mapping)
        else:
            # If no mapping found, create empty mapper
            mapper = SQLMapper({}, {})
        
        # Process q1
        if q1:
            q1_mapped = mapper.map_sql_query(q1, pretty=False)
            df.at[idx, 'q1_mapped'] = q1_mapped
        
        # Process q2
        if q2:
            q2_mapped = mapper.map_sql_query(q2, pretty=False)
            df.at[idx, 'q2_mapped'] = q2_mapped
        
        # Print progress every 1000 rows
        if (idx + 1) % 1000 == 0:
            print(f"Processed {idx + 1} rows...")
    
    # Save to output file if specified
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")
    
    return df


if __name__ == "__main__":
    # Process the bird.csv file
    csv_path = "/Users/xinyi/Desktop/ce_bench/datasets/bird/bird.csv"
    schemas_path = "/Users/xinyi/Desktop/ce_bench/datasets/bird/schemas.json"
    output_path = "/Users/xinyi/Desktop/ce_bench/datasets/bird/bird.csv"
    
    print("Starting to process bird.csv...")
    results = process_bird_csv(csv_path, schemas_path, output_path)
    print(f"Processing complete! Processed {len(results)} rows.")
    
    # Show some statistics
    q1_mapped_count = results['q1_mapped'].notna().sum()
    q2_mapped_count = results['q2_mapped'].notna().sum()
    q1_failed_count = (results['q1'].notna() & results['q1_mapped'].isna()).sum()
    q2_failed_count = (results['q2'].notna() & results['q2_mapped'].isna()).sum()
    
    print(f"\nStatistics:")
    print(f"Q1 queries successfully mapped: {q1_mapped_count}")
    print(f"Q1 queries failed to map: {q1_failed_count}")
    print(f"Q2 queries successfully mapped: {q2_mapped_count}")
    print(f"Q2 queries failed to map: {q2_failed_count}")

