import sqlglot
import re
from typing import Dict, Tuple


def extract_and_map_schema(schema_sql: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract table and column names from SQLite schema, create dummy versions with direct mapping.
    
    Args:
        schema_sql: SQLite schema as string
        
    Returns:
        Tuple of (dummy_schema, mapping_dict) where mapping_dict maps original names to dummy names
    """
    # Parse the SQL schema
    parsed = sqlglot.parse(schema_sql, dialect=sqlglot.dialects.SQLite)
    
    # Dictionaries to store original -> dummy mappings
    table_mapping = {}
    column_mapping = {}
    
    # Counter for generating dummy names
    table_counter = 1
    column_counter = 1
    
    # Extract table and column names
    for statement in parsed:
        if isinstance(statement, sqlglot.expressions.Create) and statement.kind == "TABLE":
            # Get table name - try different ways to extract it
            table_name = None
            if hasattr(statement.this, 'this') and hasattr(statement.this.this, 'name'):
                table_name = statement.this.this.name
            elif hasattr(statement.this, 'name'):
                table_name = statement.this.name
            else:
                # Fallback: get the table name from the string representation
                table_str = str(statement.this)
                if ' ' in table_str:
                    table_name = table_str.split()[0]
                else:
                    table_name = table_str
            
            # Create dummy table name if not already mapped
            if table_name and table_name not in table_mapping:
                dummy_table = f"t{table_counter}"
                table_mapping[table_name] = dummy_table
                table_counter += 1
            
            # Extract column names from the schema
            if statement.this.expressions:
                for expr in statement.this.expressions:
                    if isinstance(expr, sqlglot.expressions.ColumnDef):
                        column_name = expr.this.name
                        
                        # Create dummy column name if not already mapped
                        if column_name and column_name not in column_mapping:
                            dummy_column = f"c{column_counter}"
                            column_mapping[column_name] = dummy_column
                            column_counter += 1
    
    # Create direct mapping dictionary (original -> dummy)
    mapping_dict = {**table_mapping, **column_mapping}
    
    # Generate dummy schema by replacing names using word boundaries
    dummy_schema = schema_sql
    
    # Replace table names first (longer names first to avoid partial replacements)
    for original, dummy in sorted(table_mapping.items(), key=lambda x: len(x[0]), reverse=True):
        # Use word boundaries to ensure we replace whole identifiers only
        pattern = r'\b' + re.escape(original) + r'\b'
        dummy_schema = re.sub(pattern, dummy, dummy_schema)
    
    # Replace column names
    for original, dummy in sorted(column_mapping.items(), key=lambda x: len(x[0]), reverse=True):
        # Use word boundaries to ensure we replace whole identifiers only
        pattern = r'\b' + re.escape(original) + r'\b'
        dummy_schema = re.sub(pattern, dummy, dummy_schema)
    
    return dummy_schema, mapping_dict


def demo_schema_mapping():
    """Demo function to test the schema mapping"""
    schema = """CREATE TABLE CHESTS (
    CHEST_ID INT PRIMARY KEY,
    APPLE_COUNT INT,
    ORANGE_COUNT INT
);

CREATE TABLE BOXES (
    BOX_ID INT PRIMARY KEY,
    CHEST_ID INT,
    APPLE_COUNT INT,
    ORANGE_COUNT INT,
    FOREIGN KEY (CHEST_ID) REFERENCES CHESTS(CHEST_ID)
);"""
    
    dummy_schema, mapping_dict = extract_and_map_schema(schema)
    
    print("Original Schema:")
    print(schema)
    print("\n" + "="*50 + "\n")
    
    print("Dummy Schema:")
    print(dummy_schema)
    print("\n" + "="*50 + "\n")
    
    print("Mapping Dictionary:")
    # Sort by type (tables first, then columns) for better readability
    tables = {k: v for k, v in mapping_dict.items() if v.startswith('t')}
    columns = {k: v for k, v in mapping_dict.items() if v.startswith('c')}
    
    print("Tables:")
    for original, dummy in tables.items():
        print(f"  '{original}' -> '{dummy}'")
    
    print("Columns:")
    for original, dummy in columns.items():
        print(f"  '{original}' -> '{dummy}'")
    
    return dummy_schema, mapping_dict


if __name__ == "__main__":
    demo_schema_mapping()