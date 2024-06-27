# In a green county, the Democratic, Republican, and Libertarian candidates' vote 
# counts exactly match between the Harvard CVR data, MEDSL CVR data, and precinct 
# data, for every contest that falls under any of the following six offices: 
# US PRESIDENT, US SENATE, US HOUSE, GOVERNOR, STATE SENATE, and STATE HOUSE.
# 
# A county is considered yellow if all of the following conditions are satisfied, 
#  for every candidate of those three parties and every contest in those six offices:
# 	- Harvard's nonmissing CVR data are within 10% of the precinct totals
# 	- MEDSL's nonmissing CVR data are within 10% of the precinct totals
# 	- The number of rows in the precinct data that do not appear in Harvard's data 
#      are less than 20% of the total number of rows
# 	- The number of rows in the precinct data that do not appear in MEDSL's data 
#      are less than 20% of the total number of rows
# 
# A red county is any county not satisfying the conditions for a green or yellow county.


import pandas as pd
import os


################################################################################
# Global variables
################################################################################
# working directory is source directory
DATA_DIR = '../../Dropbox/CVR_parquet/' # TODO: need to be flexible to other users
OUT_DIR = 'status/'
OUT_DIR2 = 'status/counties-classified/'

#Define the difference and missingness proportions allowed for a yellow county
Y_DIFF_TAU = 0.1
Y_MISS_TAU = 0.2


################################################################################
# Compare
################################################################################
both = pd.read_excel(DATA_DIR + 'combined/compare.xlsx', sheet_name = "by-cand-coalesced")

#Filter by party and create difference variables
both = both.loc[(both.party_detailed == 'DEMOCRAT') |
                (both.party_detailed == 'REPUBLICAN') |
                (both.party_detailed == 'LIBERTARIAN')]
both.reset_index(inplace=True)
both['diff_c'] = abs(both.votes_v - both.votes_c)
both['diff_c_prop'] = both.diff_c / both.votes_c

county_cols = {}
gSum = 0
ySum = 0
rSum = 0
for state in both.state.unique():
    county_cols[state] = {}
    counties = both.loc[both.state == state, 'county_name'].unique()
    for county in counties:
        #Construct a series of all relevant differences
        cDiffs = both.loc[(both.state == state) &
                               (both.county_name == county),
                               'diff_c_prop']
        cDiffs = cDiffs.fillna(1)
        #Green counties require exact matches in the whole series
        if all([all(cDiffs == 0)]):
            county_cols[state][county] = 'green'
            gSum += 1
        #Apply the definition for a yellow county
        elif all([all(cDiffs.loc[cDiffs != 1] < Y_DIFF_TAU),
                  sum(cDiffs == 1) < Y_MISS_TAU*len(cDiffs)
                  ]):
            county_cols[state][county] = 'yellow'
            ySum += 1
        #Everything else is red
        else:
            county_cols[state][county] = 'red'
            rSum += 1

for state in county_cols.keys():
    fName = OUT_DIR2 + state + '.txt'
    #Write each category to a file, formatted with spaces and indentation
    greens = [k for k,v in county_cols[state].items() if v == 'green']
    yellows = [k for k,v in county_cols[state].items() if v == 'yellow']
    reds = [k for k,v in county_cols[state].items() if v == 'red']
    if len(greens)+len(yellows)+len(reds) != len(county_cols[state]):
        print(f"UNIT CHECK FAILED! Some counties in {state} not classified?")
    with open(fName, "w") as f:
        f.write("GREEN COUNTIES:\n")
        for g in greens:
            f.write(f"\t{g}\n")
        f.write("\nYELLOW COUNTIES:\n")
        for y in yellows:
            f.write(f"\t{y}\n")
        f.write("\nRED COUNTIES:\n")
        for r in reds:
            f.write(f"\t{r}\n")
f.close()

#Build one spreadsheet with three variables: state, county, and colour
with open(DATA_DIR+'combined/classifications.csv', 'w') as f:
    f.write("state,county,colour\n")
    for state in county_cols.keys():
        for county in county_cols[state].keys():
            colour = county_cols[state][county]
            newRow = f"{state},{county},{colour}\n"
            f.write(newRow)
f.close()

print(f"Green: {gSum}\nYellow: {ySum}\nRed: {rSum}")
with open(OUT_DIR+'colors.txt', 'w') as f:
    f.write(f"Green: {gSum}\nYellow: {ySum}\nRed: {rSum}")
