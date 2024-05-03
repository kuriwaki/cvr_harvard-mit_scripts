# python parse_pagination.py -i cvr.csv

import argparse
import pandas as pd

# Define the argument parser
parser = argparse.ArgumentParser(description="Merge and clean CSV data.")
parser.add_argument("-i", "--input", required=True, help="Input CSV file")
parser.add_argument("-c", "--col", required=True, help="Column Name")

# Parse arguments
args = parser.parse_args()

# print out the arguments
print(args.input)

# Read data from the input file

# if Rhode Island, set encoding to 'ISO-8859-1', otherwise utf-8
if 'ri' in args.input:
    data = pd.read_csv(args.input, dtype=str, encoding='ISO-8859-1')
else:
    data = pd.read_csv(args.input, dtype=str)

# remove [2] from the args.col column, this deals with Rhode Island
data[args.col] = data[args.col].str.replace(r"\[.*\]", "")

def is_split_row(current_row, next_row):
    """
    Check if the current row is part of a split row.
    A row is considered split if it has NaN in some columns and the next row has corresponding non-NaN values.
    """
    # Check for NaN in current row and non-NaN in next row in the same columns
    split_indicator = current_row.isna() & next_row.notna()
    return split_indicator.any()

def merge_rows(row1, row2):
    """
    Merge two rows, where the second row fills in the NaN values of the first row.
    """
    # Use 'combine_first' to merge while preserving non-NaN values in row1
    return row1.combine_first(row2)

# Initialize a DataFrame for cleaned data
cleaned_data = pd.DataFrame(columns=data.columns)

def merge_groups(group):
    # Merging all rows of the group into one
    merged_row = group.ffill().iloc[-1]  # Forward fill to propagate non-NaN values
    return merged_row

# Group the data by contiguous args.col
grouped = data.groupby((data[args.col] != data[args.col].shift()).cumsum())

# List to accumulate merged rows
accumulated_rows = []

for _, group in grouped:
    merged_row = merge_groups(group)
    accumulated_rows.append(merged_row)

# Concatenate all merged rows at once
cleaned_data = pd.concat(accumulated_rows, axis=1).transpose()
cleaned_data.reset_index(drop=True, inplace=True)

# Find the index of the args.col column
ballot_type_index = cleaned_data.columns.get_loc(args.col)

# Determine the column immediately to the right of args.col
target_column = cleaned_data.columns[ballot_type_index + 1]

# Set to keep track of used rows
used_rows = set()

# Iterate through the DataFrame
for i, current_row in cleaned_data.iterrows():
    # Skip if the row has been used already
    if i in used_rows:
        continue

    # Check if the current row is missing values in the target column
    if pd.isna(current_row[target_column]):
        closest_match = None
        closest_distance = float('inf')

        # Search for the closest complementary row
        # Iterate only within the defined range
        for j in range(max(0, i - 100), min(len(cleaned_data), i + 101)):
            if i != j and j not in used_rows:
                complementary_row = cleaned_data.iloc[j]

                # Check if the row meets the criteria
                if current_row[args.col] == complementary_row[args.col] and not pd.isna(complementary_row[target_column]):
                    distance = abs(i - j)
                    if distance < closest_distance:
                        closest_distance = distance
                        closest_match = j

        if closest_match is not None:
            # Merge the rows
            merged_row = merge_rows(current_row, cleaned_data.iloc[closest_match])
            cleaned_data.loc[i] = merged_row
            used_rows.add(closest_match)

# Save the post-processed data to the output file
cleaned_data.drop(used_rows).to_csv(args.input.replace(".csv", "_merged.csv"), index=False)
