#!/usr/bin/env python
# coding: utf-8

import os, sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

path = os.path.join("/home/gridsan/groups/cvrs", sys.argv[1].lstrip('./'))
df = pd.read_csv(path, encoding='ISO-8859-1', low_memory=False)

df.drop(df.index[-1], inplace=True) #there's a weird extra row at the end

#extracting town names from precinct names
df['town'] = df['Precinct'].astype(str).str.extract(r'^(\D+)')
df['town'] = df['town'].str.strip()
df['town'] = df['town'].str.replace(' Presidential', '')
df['town'] = df['town'].str.replace(' Limited', '')
df['town'] = df['town'].str.replace(' Precinct #', '')

#I found it the easiest to manually go through inidividual elections in excel to efficiently label office and district and then load the file back up
df_dictionary = pd.read_csv('/home/gridsan/groups/cvrs/code/cvrs/util/ri_election_dictionary_manual.csv', low_memory=False)

#melting
melted_df = pd.melt(df, id_vars=['Cast Vote Record', 'Precinct', 'Ballot Style', 'town'], var_name='election', value_name='candidate', ignore_index=True)

#dropping rows that were created from empty cells, that's the cells without votes cast
melted_df.dropna(subset = ['candidate'], inplace=True)

# left join to get all the dictionary columns
melted_df = melted_df.merge(df_dictionary, on='election', how='left')

parties = ['AS', 'All', 'DEM', 'GRN', 'GG', 'Ind', 'Lib', 'REP', 'No', 'Approve', 'Reject', 'S&L', 'Yes', 'overvote', 'undervote']

#extracting party names from candidates
def add_party_column(df, list_of_parties):
    df['party'] = df['candidate'].str.split().str[0]
    df['party'] = df['party'].where(df['party'].isin(list_of_parties), 'NONPARTISAN')
    df['party'] = df['party'].where(~df['candidate'].isin(['overvote', 'undervote']), '')
    df['party'] = df['party'].where(~df['candidate'].isin(['Yes', 'No', 'Approve', 'Reject']), '')
    return df

list_of_parties = ['AS', 'All', 'DEM', 'GRN', 'GG', 'Lib', 'REP', 'S&L']
melted_df = add_party_column(melted_df, list_of_parties)

#removing parties from candidate names
def remove_starting_words(words_list, dataframe_column):
    for word in words_list:
        dataframe_column = dataframe_column.apply(lambda x: x.replace(word + ' ', '') if x.startswith(word + ' ') else x)
    return dataframe_column

melted_df['candidate'] = remove_starting_words(list_of_parties+['Ind'], melted_df['candidate'])

#cleaning candidate names
melted_df['candidate'] = melted_df['candidate'].apply(lambda x: x.replace(' President', ''))
melted_df['candidate'] = melted_df['candidate'].apply(lambda x: x.replace('President', ''))
melted_df['candidate'] = melted_df['candidate'].str.replace(r' \(.+?\)', '', regex=True)
melted_df['candidate'] = melted_df['candidate'].str.upper()

melted_df['district'] = melted_df.apply(lambda row: row['district'].zfill(3) if row['office'] in ['US HOUSE', 'STATE HOUSE', 'STATE SENATE'] else row['district'], axis=1)
melted_df['district'] = melted_df.apply(lambda row: 'FEDERAL' if row['office'] == 'US PRESIDENT' else row['district'], axis=1)
melted_df['district'] = melted_df.apply(lambda row: 'RHODE ISLAND' if row['office'] == 'US SENATE' else row['district'], axis=1)

town_to_county = {'Barrington' : 'Bristol',
'Bristol' : 'Bristol',
'Burrillville' : 'Providence',
'Central Falls' : 'Providence',
'Charlestown' : 'Washington',
'Coventry' : 'Kent',
'Cranston' : 'Providence',
'Cumberland' : 'Providence',
'East Greenwich' : 'Kent',
'East Providence' : 'Providence',
'Exeter' : 'Washington',
'Foster' : 'Providence',
'Glocester' : 'Providence',
'Hopkinton' : 'Washington',
'Jamestown' : 'Newport',
'Johnston' : 'Providence',
'Lincoln' : 'Providence',
'Little Compton' : 'Newport',
'Middletown' : 'Newport',
'Narragansett' : 'Washington',
'Newport' : 'Newport',
'New Shoreham' : 'Washington',
'North Kingstown' : 'Washington',
'North Providence' : 'Providence',
'North Smithfield' : 'Providence',
'Pawtucket' : 'Providence',
'Portsmouth' : 'Newport',
'Providence' : 'Providence',
'Richmond' : 'Washington',
'Scituate' : 'Providence',
'Smithfield' : 'Providence',
'South Kingstown' : 'Washington',
'Tiverton' : 'Newport',
'Warren' : 'Bristol',
'Warwick' : 'Kent',
'Westerly' : 'Washington',
'West Greenwich' : 'Kent',
'West Warwick' : 'Kent',
'Woonsocket' : 'Providence',
'Federal' : ''
}


town_to_fips = {'BARRINGTON' : '4400105140',
'BRISTOL' : '4400109280',
'CENTRAL FALLS' : '4400714140',
'CUMBERLAND' : '4400720080',
'EAST PROVIDENCE' : '4400722960',
'JAMESTOWN' : '4400536820',
'LINCOLN' : '4400741500',
'LITTLE COMPTON' : '4400542400',
'MIDDLETOWN' : '4400545460',
'NEWPORT' : '4400549960',
'NORTH PROVIDENCE' : '4400751760',
'NORTH SMITHFIELD' : '4400752480',
'PAWTUCKET' : '4400754640',
'PORTSMOUTH' : '4400557880',
'PROVIDENCE' : '4400759000',
'SMITHFIELD' : '4400766200',
'TIVERTON' : '4400570880',
'WARREN' : '4400173760',
'WOONSOCKET' : '4400780780',
'BURRILLVILLE' : '4400711800',
'CHARLESTOWN' : '4400914500',
'COVENTRY' : '4400318640',
'CRANSTON' : '4400719180',
'EAST GREENWICH' : '4400322240',
'EXETER' : '4400925300',
'FOSTER' : '4400727460',
'GLOCESTER' : '4400730340',
'HOPKINTON' : '4400935380',
'JOHNSTON' : '4400737720',
'NARRAGANSETT' : '4400948340',
'NEW SHOREHAM' : '4400950500',
'NORTH KINGSTOWN' : '4400951580',
'RICHMOND' : '4400961160',
'SCITUATE' : '4400764220',
'SOUTH KINGSTOWN' : '4400967460',
'WARWICK' : '4400374300',
'WEST GREENWICH' : '4400377720',
'WEST WARWICK' : '4400378440',
'WESTERLY' : '4400977000',
'FEDERAL' : ''}

party_dictionary = {'AS' : 'AMERICAN SOLIDARITY', 'All' : 'ALLIANCE', 'DEM' : 'DEMOCRAT', 'GRN' : 'GREEN', 'GG' : 'GG','Ind' : 'INDEPENDENT', 'Lib' : 'LIBERTARIAN', 'REP':'REPUBLICAN', 'S&L':'PARTY FOR SOCIALISM AND LIBERATION'}

#creating new dataframe with standardized column names
melted_df['state'] = 'RHODE ISLAND'
melted_df['county'] = melted_df['town'].replace(town_to_county)
melted_df['county'] = melted_df['county'].str.upper()
melted_df['party_detailed'] = melted_df['party'].replace(party_dictionary)

df_clean = pd.DataFrame()
df_clean[['cvr_id', 'state', 'county_name', 'jurisdiction_name', 'precinct',
          'office', 'party_detailed', 'candidate', 'district',
          'magnitude']] = melted_df[['Cast Vote Record', 'state', 'county', 'town',
                                    'Precinct', 'office', 'party_detailed', 'candidate',
                                    'district', 'magnitude']]

parquet_table = pa.Table.from_pandas(df_clean)

pq.write_to_dataset(
    parquet_table,
    root_path="data/pass1/",
    basename_template="part-{i}.parquet",
    partition_cols=["state", "county_name"]
    )