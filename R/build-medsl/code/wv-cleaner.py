#####################################################
#        Clean all the CVR data for Delaware        #
#    https://votedatabase.com/cvr/West%20Virginia   #
#####################################################

import os
import sys
import time
import numpy as np
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
    splitted = name.split(" ")
    party = splitted[0]
    parties = {
        "DEM" : "DEMOCRAT",
        "REP" : "REPUBLICAN",
        "LBN" : "LIBERTARIAN",
        "MTN" : "GREEN PARTY",
        "IND" : "INDEPENDENT",
        "overvote" : "",
        "undervote" : "",
        "Write-in" : "",
    }

    if party not in parties:
        return "NONPARTISAN"
    return parties[party]


def get_voted(office):
    if office == np.NaN:
        return "NA"
    else:
        return 1


def nicholas_cleaner(df):
    """
    parameters: 
        df: pandas DataFrame which contains all uncleaned data
    """
    # Delete last row since it is all blank
    df.drop(index=df.index[-1], axis=0, inplace=True)

    # Make the column headers lowercase
    df.columns = map(str.lower, df.columns)

    # Drop both columns because they failed to give a name to the office 
    df = df.rename(columns={
        "unnamed: 14" : "for member of house of delegates 32nd delegate district (2122)", 
        "unnamed: 15" : "for member of house of delegates 32nd delegate district (2122)",
        })

    new_df = pd.melt(df, 
                 id_vars=["cast vote record", "precinct"],
                 value_vars=["for president (2072)",
                                "for u.s. senator (2077)",
                                "for u.s. house of representatives 3rd dist (2082)",
                                "for governor (2087)", 
                                "for secretary of state (2092)",
                                "for auditor (2097)", 
                                "for treasurer (2102)",
                                "for commissioner of agriculture (2107)", 
                                "for attorney general (2112)",
                                "for state senator 11th dist (2117)",
                                "for member of house of delegates 32nd delegate district (2122)",
                                "for member of house of delegates 41st delegate district (2131)",
                                "for member of house of delegates 44th delegate district (2136)",
                                "nonpartisan ballot of election of family court judge - 16th div",
                                "for county commissioner (2141)", 
                                "for prosecuting attorney (2146)",
                                "for sheriff (2151)", 
                                "for assessor (2156)", 
                                "for surveyor (2161)"],
                 var_name="office",
                 value_name="candidate")

    new_df = new_df.rename(columns={"cast vote record" : "cvr_id"})
    new_df["state"] = "WEST VIRGINIA"
    new_df["county_name"] = "NICHOLAS"
    new_df["district"] = ""
    new_df["party_detailed"] = ""
    new_df["magnitude"] = 1

    offices = {
        "for president (2072)" : "US PRESIDENT",
        "for u.s. senator (2077)" : "US SENATE",
        "for u.s. house of representatives 3rd dist (2082)" : "US HOUSE",
        "for governor (2087)" : "GOVERNOR", 
        "for secretary of state (2092)" : "STATE SECRETARY",
        "for auditor (2097)" : "AUDITOR", 
        "for treasurer (2102)" : "TREASURER",
        "for commissioner of agriculture (2107)" : "COMMISSIONER OF AGRICULTURE", 
        "for attorney general (2112)" : "ATTORNEY GENERAL",
        "for state senator 11th dist (2117)" : "STATE SENATE",
        "for member of house of delegates 32nd delegate district (2122)" : "STATE HOUSE",
        "for member of house of delegates 41st delegate district (2131)" : "STATE HOUSE",
        "for member of house of delegates 44th delegate district (2136)" : "STATE HOUSE",
        "nonpartisan ballot of election of family court judge - 16th div" : "FAMILY COURT JUDGE",
        "for county commissioner (2141)" : "COUNTY COMMISSIONER", 
        "for prosecuting attorney (2146)" : "PROSECUTING ATTORNEY",
        "for sheriff (2151)" : "SHERIFF", 
        "for assessor (2156)" : "ASSESSOR", 
        "for surveyor (2161)" : "",
    }

    districts = {
        "for president (2072)" : "FEDERAL",
        "for u.s. senator (2077)" : "WEST VIRGINIA",
        "for u.s. house of representatives 3rd dist (2082)" : "3",
        "for governor (2087)" : "WEST VIRGINIA", 
        "for secretary of state (2092)" : "",
        "for auditor (2097)" : "", 
        "for treasurer (2102)" : "",
        "for commissioner of agriculture (2107)" : "", 
        "for attorney general (2112)" : "",
        "for state senator 11th dist (2117)" : "11",
        "for member of house of delegates 32nd delegate district (2122)" : "32",
        "for member of house of delegates 41st delegate district (2131)" : "41",
        "for member of house of delegates 44th delegate district (2136)" : "44",
        "nonpartisan ballot of election of family court judge - 16th div" : "",
        "for county commissioner (2141)" : "", 
        "for prosecuting attorney (2146)" : "",
        "for sheriff (2151)" : "", 
        "for assessor (2156)" : "", 
        "for surveyor (2161)" : "",
    }
    
    new_df["district"] = new_df.apply(lambda row: districts[row["office"]], axis=1)
    new_df["office"] = new_df.apply(lambda row: offices[row["office"]], axis=1)
    new_df["party_detailed"] = new_df.apply(lambda row: get_party_detailed(row["candidate"]), axis=1)
    new_df["voted"] = new_df.apply(lambda row: get_voted(row["office"]), axis=1)

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


def wood_cleaner(df):
    """
    parameters: 
        df: pandas DataFrame which contains all uncleaned data
    """
    # Delete last row since it is all blank
    df.drop(index=df.index[-1], axis=0, inplace=True)

    # Make the column headers lowercase
    df.columns = map(str.lower, df.columns)

    # Drop both columns because they failed to give a name to the office 
    df = df.rename(columns={
        "unnamed: 16" : "for member of house of delegates 10th district (3552)",
        "unnamed: 17" : "for member of house of delegates 10th district (3552)",
        "unnamed: 30" : "for council-at-large city of vienna (3606)",
        "unnamed: 31" : "for council-at-large city of vienna (3606)",
        "unnamed: 32" : "for council-at-large city of vienna (3606)",
        "unnamed: 33" : "for council-at-large city of vienna (3606)",
        "unnamed: 44" : "for city council north hills",
        "unnamed: 45" : "for city council north hills",
        "unnamed: 46" : "for city council north hills",
        "unnamed: 47" : "for city council north hills",
    })

    new_df = pd.melt(df, 
                 id_vars=["cast vote record", "precinct"],
                 value_vars=["for president (3492)",
                                "for u.s. senator (3497)",
                                "for u.s. house of representatives 1st dist (3502)",
                                "for governor (3507)", 
                                "for secretary of state (3512)",
                                "for auditor (3517)", 
                                "for treasurer (3522)",
                                "for commissioner of agriculture (3527)", 
                                "for attorney general (3532)",
                                "for state senator 3rd dist (3537)",
                                "for member of house of delegates 8th district (3542)",
                                "for member of house of delegates 9th district (3547)",
                                "for member of house of delegates 10th district (3552)", 
                                "for circuit clerk - unexpired term (3561)",
                                "for county commissioner dist c (3566)",
                                "for prosecuting attorney (3571)", 
                                "for sheriff (3576)",
                                "for assessor (3581)", 
                                "for surveyor (3586)",
                                "for mayor parkersburg (3591)", 
                                "for mayor vienna (3596)",
                                "for mayor town of north hills", 
                                "for recorder vienna (3601)",
                                "for recorder north hills",
                                "for council-at-large city of vienna (3606)", 
                                "for city council parkersburg district 1 (3611)",
                                "for city council parkersburg district 2 (3624)",
                                "for city council parkersburg district 3 (3637)",
                                "for city council parkersburg district 4 (3650)",
                                "for city council parkersburg district 5 (3663)",
                                "for city council parkersburg district 6 (3676)",
                                "for city council parkersburg district 7 (3689)",
                                "for city council parkersburg district 8 (3702)",
                                "for city council parkersburg district 9 (3715)",
                                "for city council north hills"],
                 var_name="office",
                 value_name="candidate")

    new_df = new_df.rename(columns={"cast vote record" : "cvr_id"})
    new_df["state"] = "WEST VIRGINIA"
    new_df["county_name"] = "WOOD"
    new_df["district"] = ""
    new_df["party_detailed"] = ""
    new_df["magnitude"] = 1

    offices = {
        "for president (3492)" : "US PRESIDENT",
        "for u.s. senator (3497)" : "US SENATE",
        "for u.s. house of representatives 1st dist (3502)" : "US HOUSE",
        "for governor (3507)" : "GOVERNOR", 
        "for secretary of state (3512)" : "STATE SECRETARY",
        "for auditor (3517)" : "AUDITOR", 
        "for treasurer (3522)" : "TREASURER",
        "for commissioner of agriculture (3527)" : "COMMISSIONER OF AGRICULTURE", 
        "for attorney general (3532)" : "ATTORNEY GENERAL",
        "for state senator 3rd dist (3537)" : "STATE SENATE",
        "for member of house of delegates 8th district (3542)" : "STATE HOUSE",
        "for member of house of delegates 9th district (3547)" : "STATE HOUSE",
        "for member of house of delegates 10th district (3552)" : "STATE HOUSE", 
        "for circuit clerk - unexpired term (3561)" : "CIRCUIT CLERK",
        "for county commissioner dist c (3566)" : "COUNTY COMMISSIONER",
        "for prosecuting attorney (3571)" : "PROSECUTING ATTORNEY", 
        "for sheriff (3576)" : "SHERIFF",
        "for assessor (3581)" : "ASSESSOR", 
        "for surveyor (3586)" : "SURVEYOR",
        "for mayor parkersburg (3591)" : "MAYOR", 
        "for mayor vienna (3596)" : "MAYOR",
        "for mayor town of north hills" : "MAYOR", 
        "for recorder vienna (3601)" : "RECORDER",
        "for recorder north hills" : "RECORDER",
        "for council-at-large city of vienna (3606)" : "COUNCIL AT LARGE", 
        "for city council parkersburg district 1 (3611)" : "CITY COUNCIL",
        "for city council parkersburg district 2 (3624)" : "CITY COUNCIL",
        "for city council parkersburg district 3 (3637)" : "CITY COUNCIL",
        "for city council parkersburg district 4 (3650)" : "CITY COUNCIL",
        "for city council parkersburg district 5 (3663)" : "CITY COUNCIL",
        "for city council parkersburg district 6 (3676)" : "CITY COUNCIL",
        "for city council parkersburg district 7 (3689)" : "CITY COUNCIL",
        "for city council parkersburg district 8 (3702)" : "CITY COUNCIL",
        "for city council parkersburg district 9 (3715)" : "CITY COUNCIL",
        "for city council north hills" : "CITY COUNCIL",
    }

    districts = {
        "for president (3492)" : "FEDERAL",
        "for u.s. senator (3497)" : "WEST VIRGINIA",
        "for u.s. house of representatives 1st dist (3502)" : "001",
        "for governor (3507)" : "WEST VIRGINIA", 
        "for secretary of state (3512)" : "WEST VIRGINIA",
        "for auditor (3517)" : "", 
        "for treasurer (3522)" : "",
        "for commissioner of agriculture (3527)" : "", 
        "for attorney general (3532)" : "",
        "for state senator 3rd dist (3537)" : "003",
        "for member of house of delegates 8th district (3542)" : "008",
        "for member of house of delegates 9th district (3547)" : "009",
        "for member of house of delegates 10th district (3552)" : "010", 
        "for circuit clerk - unexpired term (3561)" : "",
        "for county commissioner dist c (3566)" : "C",
        "for prosecuting attorney (3571)" : "", 
        "for sheriff (3576)" : "",
        "for assessor (3581)" : "", 
        "for surveyor (3586)" : "",
        "for mayor parkersburg (3591)" : "PARKERSBURG", 
        "for mayor vienna (3596)" : "VIENNA",
        "for mayor town of north hills" : "NORTH HILLS", 
        "for recorder vienna (3601)" : "VIENNA",
        "for recorder north hills" : "NORTH HILLS",
        "for council-at-large city of vienna (3606)" : "", 
        "for city council parkersburg district 1 (3611)" : "PARKERSBURG 1",
        "for city council parkersburg district 2 (3624)" : "PARKERSBURG 2",
        "for city council parkersburg district 3 (3637)" : "PARKERSBURG 3",
        "for city council parkersburg district 4 (3650)" : "PARKERSBURG 4",
        "for city council parkersburg district 5 (3663)" : "PARKERSBURG 5",
        "for city council parkersburg district 6 (3676)" : "PARKERSBURG 6",
        "for city council parkersburg district 7 (3689)" : "PARKERSBURG 7",
        "for city council parkersburg district 8 (3702)" : "PARKERSBURG 8",
        "for city council parkersburg district 9 (3715)" : "PARKERSBURG 9",
        "for city council north hills" : "NORTH HILLS",
    }    
    
    new_df["district"] = new_df.apply(lambda row: districts[row["office"]], axis=1)
    new_df["office"] = new_df.apply(lambda row: offices[row["office"]], axis=1)
    new_df["party_detailed"] = new_df.apply(lambda row: get_party_detailed(row["candidate"]), axis=1)
    new_df["voted"] = new_df.apply(lambda row: get_voted(row["office"]), axis=1)

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

    # Clean the counties
    df1 = nicholas_cleaner(open_csv(sys.argv[1]))
    df2 = wood_cleaner(open_csv(sys.argv[2]))
    
    df = pd.concat([df1, df2])

    parquet_table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        parquet_table,
        root_path="data/pass1/",
        basename_template="part-{i}.parquet",
        partition_cols=["state", "county_name"]
        )
    
    print("TIME TO RUN WEST VIRGINIA:", time.time() - start)