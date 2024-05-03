#####################################################
#        Clean all the CVR data for Minnesota       #
#       https://votedatabase.com/cvr/Minnesota      #
#####################################################

import os
import re
import sys
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

########################
### Helper Functions ###
########################
def open_csv(csv):
    """
    parameters: 
        csv: string that contains the path to the csv file
                example: "./CA-cleaned.csv"
    """
    path = os.path.join("/home/gridsan/groups/cvrs", csv.lstrip('./'))
    return pd.read_csv(path, encoding="utf-8", low_memory=False)


def get_party_detailed(name):
    if type(name) == float:
        return name

    parties = {
        "DFL" : "DEMOCRAT",
        "REP" : "REPUBLICAN",
        "LIB" : "LIBERTARIAN",
        "IND" : "INDEPENDENT",
        "INA" : "INDEPENDENCE ALLIANCE",
        "GRN" : "GREEN PARTY",
        "SWP" : "SOCIALIST WORKER",
        "GLC" : "GRASSROOTS-LEGALIZE CANNABIS",
        "LMN" : "LEGAL MARIJUANA NOW",
        "undervote" : "",
        "overvote" : "",
        "write-in:" : "",
    }

    split_name = name.split(" ")
    party = split_name[0]
    if len(party) == 3:
        if party in parties:
            return parties[party]
    return "NONPARTISAN"


def standardize_office(office):
    match_office = re.match(r"president and vice-president \((.*)\)(.*)", office)
    if match_office:
        return "US PRESIDENT", "FEDERAL"
    
    match_office = re.match(r"u.s. senator \((.*)\)(.*)", office)
    if match_office:
        return "US SENATE", "MINNESOTA"
    
    match_office = re.match(r"u.s. representative d1 \((.*)\)(.*)", office)
    if match_office:
        return "US HOUSE", "001"
    
    match_office = re.match(r"state senator d28(.*)", office)
    if match_office:
        return "STATE SENATE", "028"
    
    match_office = re.match(r"state representative d28(.*)", office)
    if match_office:
        return "STATE SENATE", "028"
    
    match_office = re.match(r"county commissioner d(.*) \((.*)\)(.*)", office)
    if match_office:
        return "COUNTY COMMISSIONER", f"{match_office.group(1)}"
    
    match_office = re.match(r"soil/water cnsrvtn dist sprvsr d(.*) \((.*)\)(.*)", office)
    if match_office:
        return "SOIL AND WATER CONSERVATION", f"{match_office.group(1)}"
    
    match_office = re.match(r"mayor city of (.*) \((.*)\)(.*)", office)
    if match_office:
        return "MAYOR", f"{match_office.group(1)}"
    
    match_office = re.match(r"cncl mmbr at lrg city of (.*) \((.*)\)(.*)", office)
    if match_office:
        return "COUNCIL MEMBER AT LARGE", f"{match_office.group(1)}"
    
    match_office = re.match(r"cncl mmbr at lrg rushford village (.*)yr \((.*)\)(.*)", office)
    if match_office:
        return "COUNCIL MEMBER AT LARGE", "rushford village".upper()
    
    match_office = re.match(r"sch brd mmbr isd (\d{3,4}) (.*) \((.*)\)(.*)", office)
    if match_office:
        return "SCHOOL BOARD MEMBER", f"{match_office.group(2)}"
    
    match_office = re.match(r"sbm at lrg isd (\d{3,4}) (.*) \((.*)\)(.*)", office)
    if match_office:
        return "SCHOOL BOARD MEMBER AT LARGE", f"{match_office.group(2)}"
    
    match_office = re.match(r"isd (\d{3,4}) question (\d{1,2}) \((.*)\)", office)
    if match_office:
        return "PROPOSITION", ""
    
    match_office = re.match(r"associate justice 4 \((.*)\)", office)
    if match_office:
        return "ASSOCIATE JUSTICE", ""
    
    match_office = re.match(r"judge (\d{1,2}) court of appeals \((.*)\)", office)
    if match_office:
        return "COURT OF APPEALS JUDGE", f"{match_office.group(1)}"
    
    match_office = re.match(r"judge (\d{1,2}) 3rd dist crt \((.*)\)", office)
    if match_office:
        return "COURT JUDGE", "3"
    
    match_office = re.match(r"unnamed: (\d{2,3})", office)
    if match_office:
        return "COURT JUDGE", "3"

    return "office", "district"


def cleaner(df, county):
    """
    parameters: 
        df: pandas DataFrame which contains all uncleaned data
    """
    print(f"\n{county}")
    # Make the column headers lowercase
    df.columns = map(str.lower, df.columns)

    new_df = pd.melt(df, 
                 id_vars=["cast vote record", "precinct"],
                 value_vars=list(df.columns)[2:],
                 var_name="office",
                 value_name="candidate")
        
    new_df = new_df[new_df["candidate"].notna()]
    new_df = new_df[new_df.candidate != " if any"]

    new_df = new_df.rename(columns={"cast vote record" : "cvr_id"})
    new_df["state"] = "MINNESOTA"
    new_df["county_name"] = county
    new_df["district"] = ""
    new_df["party_detailed"] = ""
    new_df["magnitude"] = "1"
    new_df["voted"] = "1"

    new_df["district"] = new_df.apply(lambda row: standardize_office(row["office"])[1], axis=1)
    new_df["office"] = new_df.apply(lambda row: standardize_office(row["office"])[0], axis=1)
    new_df["party_detailed"] = new_df.apply(lambda row: get_party_detailed(row["candidate"]), axis=1)

    new_df["jurisdiction_name"] = new_df["county_name"]
    
    new_df = new_df[[
        "cvr_id",
        "state",
        "county_name",
        "jurisdiction_name",
        "precinct",
        "office",
        "party_detailed",
        "candidate",
        "district",
        "magnitude",
        "voted"
    ]]

    return new_df[new_df["candidate"].notna()]


######################################
### Save the results to a CSV file ###
######################################
if __name__ == "__main__":
    start = time.time()
    
    # Clean the counties
    file_path = sys.argv[1]
    county_name = os.path.basename(os.path.dirname(file_path)).upper()

    df = cleaner(open_csv(file_path), county_name)

    parquet_table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        parquet_table,
        root_path="data/pass1/",
        basename_template="part-{i}.parquet",
        partition_cols=["state", "county_name"]
        )
    
    print("TIME TO RUN MINNESOTA:", time.time() - start)
