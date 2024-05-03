#####################################################
#        Clean all the CVR data for Maryland        #
#       https://votedatabase.com/cvr/Virginia       #
#####################################################

import os, re, time
import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET

########################
### Helper Functions ###
########################
def open_xml(xml):
    tree = ET.parse(xml)
    root = tree.getroot()

    all_office_candidate = []
    for a in root:
        for b in a:
            office_candidate = []
            for c in b:
                # Name tag has text office
                if c.tag == "{http://tempuri.org/CVRDesign.xsd}Name":
                    office_candidate.append(c.text)
                for d in c:
                    for e in d:
                        # Name tag has candidate office
                        if e.tag == "{http://tempuri.org/CVRDesign.xsd}Name":
                            office_candidate.append(e.text)
            if len(office_candidate) == 2:
                all_office_candidate.append(office_candidate)

    return all_office_candidate


def get_candidates(name):
    candidates = {
        'Donald S. Beyer Jr. - D' : "DONALD S BEYER JR",
        'Jeff A. Jordan - R' : "JEFF A JORDAN",
        'No' : "NO",
        'Mark R. Warner - D' : "MARK R WARNER",
        'DEMOCRATIC PARTY\nElectors for\nJoseph R. Biden, President and\nKamala D. Harris, Vice President' : "JOSEPH R BIDEN",
        'Yes' : "YES",
        'Daniel M. Gade - R' : "DANIEL M GADE",
        'LIBERTARIAN PARTY\nElectors for\nJo Jorgensen, President and\nJeremy F. "Spike" Cohen, Vice President' : "JO JORGENSEN",
        'REPUBLICAN PARTY\nElectors for\nDonald J. Trump, President and\nMichael R. Pence, Vice President' : "DONALD J TRUMP",
    }
    
    if name not in candidates:
        return ""
    return candidates[name]


def get_party_detailed(name):
    if type(name) == float:
        return name
    
    parties = {
        'Donald S. Beyer Jr. - D' : "DEMOCRAT",
        'Jeff A. Jordan - R' : "REPUBLICAN",
        'No' : "",
        'Mark R. Warner - D' : "DEMOCRAT",
        'DEMOCRATIC PARTY\nElectors for\nJoseph R. Biden, President and\nKamala D. Harris, Vice President' : "DEMOCRAT",
        'Yes' : "",
        'Daniel M. Gade - R' : "REPUBLICAN",
        'LIBERTARIAN PARTY\nElectors for\nJo Jorgensen, President and\nJeremy F. "Spike" Cohen, Vice President' : "LIBERTERIAN",
        'REPUBLICAN PARTY\nElectors for\nDonald J. Trump, President and\nMichael R. Pence, Vice President' : "REPUBLICAN",
    }

    if name not in parties:
        return ""
    return parties[name]


def standardize_office(office):
    match_office = re.match(r"President And Vice President", office)
    if match_office:
        return "US PRESIDENT", "FEDERAL"
    
    match_office = re.match(r"Member House of Representatives\n8th District", office)
    if match_office:
        return "US HOUSE", "008"
    
    match_office = re.match(r"Member\nUnited States Senate", office)
    if match_office:
        return "US SENATE", "VIRGINIA"
    
    match_office = re.match(r"Constitutional Amendment #(\d{1})", office)
    if match_office:
        return "PROPOSITION", re.search(r"Constitutional Amendment #(\d{1})", office).group(1)

    return "office", "district"


def cleaner(df, county):
    """
    parameters: 
        df: pandas DataFrame which contains all uncleaned data
    """
    new_df = df
    
    new_df["state"] = "VIRGINIA"
    new_df["precinct"] = ""
    new_df["county_name"] = county
    new_df["district"] = ""
    new_df["party_detailed"] = ""
    new_df["magnitude"] = "1"
    new_df["voted"] = "1"

    new_df["district"] = new_df.apply(lambda row: standardize_office(row["office"])[1], axis=1)
    new_df["office"] = new_df.apply(lambda row: standardize_office(row["office"])[0], axis=1)
    new_df["party_detailed"] = new_df.apply(lambda row: get_party_detailed(row["candidate"]), axis=1)
    new_df["candidate"] = new_df.apply(lambda row: get_candidates(row["candidate"]), axis=1)

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


########################################
### Save the results to parquet file ###
########################################
if __name__ == "__main__":
    # To run the file in the terminal: 
    # python3 cvr-cleaner.py

    start = time.time()

    path = os.path.join("/home/gridsan/groups/cvrs", sys.argv[1].lstrip('./'))

    all_office_candidate = []
    for filename in os.listdir(path):
        if filename.endswith(".xml"): 
            fullname = os.path.join(path, filename)
            parsed = open_xml(fullname)
            all_office_candidate.extend(parsed)

    for i in range(len(all_office_candidate)):
        all_office_candidate[i] = [i] + all_office_candidate[i]

    df = pd.DataFrame(all_office_candidate, columns=["cvr_id", "office", "candidate"])

    df = cleaner(df, "")

    parquet_table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        parquet_table,
        root_path="data/pass1/",
        basename_template="part-{i}.parquet",
        partition_cols=["state", "county_name"]
        )
    
    print("TIME TO RUN VIRGINIA:", time.time() - start)
    