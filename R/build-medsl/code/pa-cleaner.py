#####################################################
#        Clean all the CVR data for Pennsylvania    #
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
    path = os.path.join("/home/gridsan/groups/cvrs/data/raw/Pennsylvania/Allegheny", csv.lstrip('./'))
    return pd.read_csv(path, encoding="utf-8", low_memory=False)


def get_party_detailed(name):
    if type(name) == float:
        return name
    
    parties = {
        'Conor Lamb (15465)' : "DEMOCRAT", 
        'Daniel Wassmer (19080)' : "LIBERTARIAN", 
        'Devlin Robinson (15479)' : "REPUBLICAN", 
        'Donald J. Trump/Mike R. Pence (15437)' : "REPUBLICAN", 
        'Heather Heidelbaugh (15445)' : "REPUBLICAN", 
        'Jay Costa, Jr. (15481)' : "DEMOCRAT", 
        'Jennifer Moore (19084)' : "LIBERTARIAN", 
        'Jo Jorgensen/Jeremy Spike Cohen (19076)' : "LIBERTARIAN", 
        'Joe Soloski (19088)' : "LIBERTARIAN", 
        'Joe Torsella (15461)' : "DEMOCRAT", 
        'Joseph R. Biden/Kamala D. Harris (15431)' : "DEMOCRAT", 
        'Josh Shapiro (15443)' : "DEMOCRAT", 
        'Luke Edison Negron (15473)' : "REPUBLICAN", 
        'Mike Doyle (15469)' : "DEMOCRAT", 
        'Nina Ahmad (15449)' : "DEMOCRAT", 
        'No (21773)' : "NONPARTISAN", 
        'Olivia Faison (19082)' : "GREEN PARTY", 
        'Pam Iovino (15475)' : "DEMOCRAT", 
        'Richard L. Weiss (19078)' : "GREEN PARTY", 
        'Sean Parnell (15467)' : "REPUBLICAN", 
        'Stacy L. Garrity (15463)' : "REPUBLICAN", 
        'Timothy DeFoor (15459)' : "REPUBLICAN", 
        'Timothy Runkle (19086)' : "GREEN PARTY", 
        'Yes (21771)' : "NONPARTISAN",
        "undervote" : "",
        "overvote" : "",
        'Write-in (18958)' : "", 
    }

    if name not in parties:
        return ""
    return parties[name]


def standardize_office(office):
    offices = {
        'Attorney General (18937)' : ["ATTORNEY GENERAL", "PENNSYLVANIA"], 
        'Auditor General (18942)' : ["AUDITOR GENERAL", "PENNSYLVANIA"], 
        'Pittsburgh HRC Amendment (21769)' : ["PROPOSITION", "PITTSBURGH"], 
        'Presidential Electors (18932)' : ["US PRESIDENT", "FEDERAL"], 
        'Representative in Congress 17th District (18952)' : ["US HOUSE", "017"], 
        'Representative in Congress 18th District (18956)' : ["US HOUSE", "018"], 
        'Senator in the General Assembly 37th District (18961)' : ["STATE SENATE", "037"], 
        'Senator in the General Assembly 43rd District (18965)' : ["STATE SENATE", "043"], 
        'State Treasurer (18947)' : ["STATE TREASURER", "PENNSYLVANIA"],
    }

    return offices[office]


def cleaner(df, county):
    """
    parameters: 
        df: pandas DataFrame which contains all uncleaned data
    """
    new_df = df

    new_df["state"] = "PENNSYLVANIA"
    new_df["county_name"] = county
    new_df["district"] = ""
    new_df["party_detailed"] = ""
    new_df["magnitude"] = "1"
    new_df["voted"] = "1"

    new_df["district"] = new_df.apply(lambda row: standardize_office(row["contest"])[1], axis=1)
    new_df["office"] = new_df.apply(lambda row: standardize_office(row["contest"])[0], axis=1)
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
    # To run the file in the terminal: 
    # python3 cvr-cleaner.py

    start = time.time()

    dfs = []
    for file in os.listdir(sys.argv[1]):
        if file.endswith(".csv"):
            df = open_csv(file)
            dfs.append(df)
    df = pd.concat(dfs)
    df["cvr_id"] = df.index

    clean = cleaner(df, "ALLEGHENY")

    parquet_table = pa.Table.from_pandas(clean)
    pq.write_to_dataset(
        parquet_table,
        root_path="data/pass1/",
        basename_template="part-{i}.parquet",
        partition_cols=["state", "county_name"]
        )
    
    print("TIME TO RUN PENNSYLVANIA:", time.time() - start)
    