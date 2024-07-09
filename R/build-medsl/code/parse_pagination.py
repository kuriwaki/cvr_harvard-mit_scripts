import argparse
import pandas as pd
from tqdm import tqdm

# Define the argument parser
parser = argparse.ArgumentParser(description="Merge and clean CSV data.")
parser.add_argument("-p", "--path", required=True, help="Path to CSV file", type=str)
parser.add_argument("-t", "--targetcol", required=True, help="Column to Compare On", type=str)
parser.add_argument("-c", "--groupcol", required=False, help="Group Column Name", type=str)

# Parse arguments
args = parser.parse_args()

# print out the path
print(args.path)

# Read data and do some initial cleanup
# if Rhode Island, set encoding to 'ISO-8859-1', otherwise utf-8
if 'ri' in args.path:
    data = pd.read_csv(args.path, dtype=str, encoding='ISO-8859-1')
else:
    data = pd.read_csv(args.path, dtype=str)

# remove [2] from the args.groupcol column, this deals with some counties that have noted the pagination explicitly
data[args.groupcol] = data[args.groupcol].str.replace(r" \[.*\]", "", regex=True)

##### 
# Function definition
#####

def merge_rows(row1, row2):
    """
    Merge two rows, where the second row fills in the NaN values of the first row.
    """
    # Use 'combine_first' to merge while preserving non-NaN values in row1
    return row1.combine_first(row2)

def iterate_spiral(j):
    increase = True
    k=j

    while True:
        if k == 0:
            j += 1
            yield j
        elif increase:
            j += 1
            yield j
            increase = False
        else:
            k -= 1
            yield k
            increase = True

## For all data, we then proceed through an iterative process of finding a good nearby match
## - this works by identifying each row that is missing values in the first column and grouping it with a nearby column that is not missing data there

# Set to keep track of used rows
used_rows = set()
num_rows = len(data)

for i, current_row in tqdm(data.iterrows(), total=num_rows, desc=args.path, unit='row', ncols=80, mininterval=1, dynamic_ncols=True):
    # Skip if the row has been used already
    if i in used_rows:
        continue

    # Check if the current row is missing values in the target column
    if pd.isna(current_row[args.targetcol]):

        # Search for the closest complementary row
        # Iterate in an expanding manner
        for j in iterate_spiral(i):
            # add a failsafe in case we find nothing
            if j >= num_rows:
                break

            if j not in used_rows and current_row[args.groupcol] == data.loc[j, args.groupcol] and not pd.isna(data.loc[j, args.targetcol]):
                # merge the rows
                merged_row = merge_rows(current_row, data.iloc[j])
                data.loc[i] = merged_row
                used_rows.add(j)

                break
            

# Save the post-processed data to the output file
data.drop(used_rows).to_csv(args.path.replace(".csv", "_merged.csv"), index=False)