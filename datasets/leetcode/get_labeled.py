import csv
import ast
import pandas as pd

# Read the CSV file
df = pd.read_csv('leetcode_full_with_states.csv')

# Add a new 'correct' column
df['correct'] = None

# Process each row to determine correctness
for i, row in df.iterrows():
    try:
        states_str = row.get('states', '[]')
        
        # Parse the states string back to a list
        try:
            if states_str and str(states_str).strip() and str(states_str) != 'None' and str(states_str) != 'nan':
                states = ast.literal_eval(str(states_str))
            else:
                states = []
        except (ValueError, SyntaxError):
            states = []
        
        # Determine correctness based on states
        if states and states[-1] == "TMO":
            correct = "Y"
        elif states and any(state in ["NEQ", "SYN", "NSE", "NIE"] for state in states):
            correct = "N"
        elif states and all(state == "EQU" for state in states):
            correct = "Y"
        else:
            correct = None  # Uncertain cases
        
        df.at[i, 'correct'] = correct

    except Exception as e:
        print(f"Error processing row {i}: {e}")
        df.at[i, 'correct'] = None

# Save the updated DataFrame back to the CSV file
df.to_csv('leetcode_full_with_states.csv', index=False)
print(f"Updated leetcode_full_with_states.csv with 'correct' column")

# Print some statistics
correct_counts = df['correct'].value_counts()
print(f"Label statistics:")
print(correct_counts)
