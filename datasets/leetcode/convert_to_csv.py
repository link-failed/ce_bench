import json
import csv
import os
from pathlib import Path

def extract_db_id(file_path):
    """Extract database ID from file path like 'benchmark/leetcode/raw_data/175.csv' -> '175'"""
    # Get the filename without extension
    filename = Path(file_path).stem
    return filename

def convert_jsonlines_to_csv(input_file, output_file):
    """Convert jsonlines file to CSV with required format"""
    
    with open(input_file, 'r', encoding='utf-8') as f_in:
        with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
            # Define CSV headers - updated for .out file structure
            fieldnames = ['dbid', 'index', 'schema', 'constraint', 'states', 'times', 'counterexample', 'err', 'q1', 'q2']
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            
            for line in f_in:
                # Parse JSON line
                data = json.loads(line.strip())
                
                # Extract dbid from file path
                dbid = extract_db_id(data['file'])
                
                # Extract q1 and q2 from pair array
                pair = data['pair']
                q1 = pair[0] if len(pair) > 0 else ""
                q2 = pair[1] if len(pair) > 1 else ""
                
                # Create CSV row
                row = {
                    'dbid': dbid,
                    'index': data['index'],
                    'schema': json.dumps(data['schema']),  # Keep as JSON string
                    'constraint': json.dumps(data['constraint']),  # Keep as JSON string
                    'states': json.dumps(data['states']),  # Keep as JSON string
                    'times': json.dumps(data['times']),  # Keep as JSON string
                    'counterexample': data.get('counterexample', ''),  # Use get() in case it's null
                    'err': data.get('err', ''),  # Use get() in case it's null
                    'q1': q1,
                    'q2': q2
                }
                
                writer.writerow(row)

if __name__ == "__main__":
    input_file = "leetcode.out"
    output_file = "leetcode.csv"
    
    convert_jsonlines_to_csv(input_file, output_file)
    print(f"Successfully converted {input_file} to {output_file}")
    
    # Show first few rows as verification
    print("\nFirst 3 rows of the converted CSV:")
    with open(output_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= 3:
                break
            print(f"Row {i+1}:")
            for key, value in row.items():
                if key in ['q1', 'q2']:
                    print(f"  {key}: {value[:100]}..." if len(value) > 100 else f"  {key}: {value}")
                else:
                    print(f"  {key}: {value}")
            print()