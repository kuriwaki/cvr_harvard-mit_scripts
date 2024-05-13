import pandas as pd
import os


################################################################################
# Global variables
################################################################################
BASE_DIR = '../'
DATA_DIR = BASE_DIR + 'combined/'
OUT_DIR = BASE_DIR + 'validation/templates/'

#Define the difference and missingness proportions allowed for a yellow county
Y_DIFF_TAU = 0.1
Y_MISS_TAU = 0.2


################################################################################
# Compare
################################################################################
both = pd.read_excel(DATA_DIR + 'compare.xlsx')

#Filter by party and create difference variables
both = both.loc[(both.party_detailed == 'DEMOCRAT') |
                (both.party_detailed == 'REPUBLICAN') |
                (both.party_detailed == 'LIBERTARIAN')]
both.reset_index(inplace=True)
both['diff_h'] = abs(both.votes_v - both.votes_h)
both['diff_m'] = abs(both.votes_v - both.votes_m)
both['diff_h_prop'] = both.diff_h/both.votes_v
both['diff_m_prop'] = both.diff_m/both.votes_v

county_cols = {}
gSum = 0
ySum = 0
rSum = 0
for state in both.state.unique():
    county_cols[state] = {}
    counties = both.loc[both.state == state, 'county_name'].unique()
    for county in counties:
        #Construct a series of all relevant differences
        hDiffs = both.loc[(both.state == state) &
                               (both.county_name == county),
                               'diff_h_prop']
        mDiffs = both.loc[(both.state == state) &
                               (both.county_name == county),
                               'diff_m_prop']
        hDiffs = hDiffs.fillna(1)
        mDiffs = mDiffs.fillna(1)
        #Green counties require exact matches in the whole series
        if all([all(hDiffs == 0), all(mDiffs == 0)]):
            county_cols[state][county] = 'green'
            gSum += 1
        #Apply the definition for a yellow county
        elif all([all(hDiffs.loc[hDiffs != 1] < Y_DIFF_TAU),
                  all(mDiffs.loc[mDiffs != 1] < Y_DIFF_TAU),
                  sum(hDiffs == 1) < Y_MISS_TAU*len(hDiffs),
                  sum(mDiffs == 1) < Y_MISS_TAU*len(mDiffs)
                  ]):
            county_cols[state][county] = 'yellow'
            ySum += 1
        #Everything else is red
        else:
            county_cols[state][county] = 'red'
            rSum += 1

for state in county_cols.keys():
    fName = OUT_DIR + state + '.txt'
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
with open(BASE_DIR+'validation/classifications.csv', 'w') as f:
    f.write("state,county,colour\n")
    for state in county_cols.keys():
        for county in county_cols[state].keys():
            colour = county_cols[state][county]
            newRow = f"{state},{county},{colour}\n"
            f.write(newRow)
f.close()

#How many yellows are we assigning to people by state?
with open(BASE_DIR+'validation/summary.txt', 'w') as f:
    for state in county_cols.keys():
        gCount = sum([v=='green' for k,v in county_cols[state].items()])
        yCount = sum([v=='yellow' for k,v in county_cols[state].items()])
        rCount = sum([v=='red' for k,v in county_cols[state].items()])
        f.write(f"{state}: {gCount} green counties, "+\
                         f"{yCount} yellow counties, "+\
                         f"{rCount} red counties.\n")
f.close()

print(f"Green: {gSum}\nYellow: {ySum}\nRed: {rSum}")
