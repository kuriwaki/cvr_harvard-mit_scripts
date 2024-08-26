#!/bin/bash
source /etc/profile

# load the required modules
module load anaconda/2023b
source activate customR

# Find .zip files in data/raw/ and its subdirectories
# do it a few times to make it handle nested directories
find data/raw/ -type f -name "*.zip" -execdir sh -c 'unzip -o "{}" && rm "{}"' \;
find data/raw/ -type f -name "*.zip" -execdir sh -c 'unzip -o "{}" && rm "{}"' \;
find data/raw/ -type f -name "*.zip" -execdir sh -c 'unzip -o "{}" && rm "{}"' \;

# remove bad files
find data/raw/ -type f -name "*.sig" -delete
find data/raw/ -type f -name "*.png" -delete
find data/raw/ -type f -name "*.pdf" -delete

# remove empty directories
find data/raw/ -type d -empty -delete

# export tree of directories to file
tree data/raw/ --filelimit=92 -H data/raw/ > raw_tree.html

## Fix split-row CVRs

## MUST MANUALLY ADD ONE ROW TO THE TOP OF Colorado/Eagle because they deleted it for some reason

# parse headers first
parallel -j 4 Rscript code/process_headers.R ::: \
    "data/raw/California/Alameda/cvr.csv" \
    "data/raw/California/Contra Costa/cvr.csv" \
    "data/raw/California/King/cvr.csv" \
    "data/raw/California/San Mateo/cvr.csv" \
    "data/raw/California/Santa Clara/cvr.csv" \
    "data/raw/California/Sonoma/cvr.csv" \
    "data/raw/California/Yuba/cvr.csv" \
    "data/raw/Colorado/Denver/cvr.csv" \
    "data/raw/Colorado/Eagle/cvr.csv" \
    "data/raw/Colorado/Routt/cvr.csv" \
    "data/raw/Georgia/Gwinnett/Nov_General_CVR_Export_20210104103413.csv" \
    "data/raw/Ohio/Butler/cvr.csv" \
    "data/raw/Ohio/Greene/cvr.csv"

# then convert them to their unpaginated form
# run this locally, it is many magnitudes faster than on Supercloud
python code/parse_pagination.py --path data/raw/California/Alameda/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph r Biden and Kamala d Harris" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/California/Contra\ Costa/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph r Biden Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/California/King/cvr_headers.csv --targetcol "President of the United States Vote for 1 Joseph r Biden and Kamala d Harris Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/California/Merced/cvr.csv --targetcol "UNITED STATES REPRESENTATIVE, DISTRICT 16 Countywide (1734)" --groupcol "Ballot Style"
python code/parse_pagination.py --path data/raw/California/San\ Mateo/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph r Biden Kamala d Harris Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/California/Santa\ Clara/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph r Biden Kamala d Harris Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/California/Sonoma/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph p Biden Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/California/Yuba/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph r Biden and Kamala d Harris Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Colorado/Denver/cvr_headers.csv --targetcol "Presidential Electors Vote for1 Joseph r Biden Kamala d Harris Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Colorado/Eagle/cvr_headers.csv --targetcol "Presidential Electors Vote for 1 Joseph r Biden Kamala d Harris" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Colorado/Routt/cvr_headers.csv --targetcol "Presidential Electors Vote for 1 Joseph r Biden Kamala d Harris" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Georgia/Gwinnett/Nov_General_CVR_Export_20210104103413_headers.csv --targetcol "President of the United States Presidentede Los Estados Unidos Vote for1 Donald j Trump i Rep" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Maryland/Baltimore/cvr.csv --targetcol "President - Vice Pres (1)" --groupcol "Ballot Style"
python code/parse_pagination.py --path data/raw/Maryland/Baltimore\ City/cvr.csv --targetcol "President - Vice Pres (1)" --groupcol "Ballot Style"
python code/parse_pagination.py --path data/raw/Maryland/Montgomery/cvr.csv --targetcol "President - Vice Pres (1)" --groupcol "Ballot Style"
python code/parse_pagination.py --path data/raw/Maryland/Prince\ Georges/cvr.csv --targetcol "President - Vice Pres (1)" --groupcol "Ballot Style"
python code/parse_pagination.py --path data/raw/Ohio/Butler/cvr_headers.csv --targetcol "President United States Vote for 1 Joseph r Biden Jr" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Ohio/Champaign/cvr.csv --targetcol "Choice_1_2:President:Vote For 1:Biden / Harris:Democratic" --groupcol "BallotStyleID"
python code/parse_pagination.py --path data/raw/Ohio/Cuyahoga/cvr.csv --targetcol "PRESIDENT AND VICE PRESIDENT" --groupcol "Ballot Style"
python code/parse_pagination.py --path data/raw/Ohio/Greene/cvr_headers.csv --targetcol "President and Vice President Vote for 1 Joseph r Biden Dem" --groupcol "Ballot Type"
python code/parse_pagination.py --path data/raw/Rhode\ Island/ri.csv --targetcol "Presidential Electors For: (29562)" --groupcol "Ballot Style"

# Manually process some weird edge cases

# Edge Case Processing
## DC
echo "DC"
find data/raw/District\ of\ Columbia -type f -name '*.xlsx' -delete

## Florida/Okaloosa
echo "Florida/Okaloosa"
if diff <(head -n 1 'data/raw/Florida/Okaloosa/Okaloosa FL part 1.csv') <(head -n 1 'data/raw/Florida/Okaloosa/Okaloosa FL part 2.csv') >/dev/null; then
    tail -n+2 'data/raw/Florida/Okaloosa/Okaloosa FL part 2.csv' >> 'data/raw/Florida/Okaloosa/Okaloosa FL part 1.csv'
    rm 'data/raw/Florida/Okaloosa/Okaloosa FL part 2.csv'
else
    echo "The first rows are different."
fi

## Georgia/Bartow
### data/raw/Georgia/Bartow/Bartow CVR_Export_20220815114818.csv is for 2020 general
echo "Georgia/Bartow"
rm 'data/raw/Georgia/Bartow/Bartow CVR_Export_20220815112021.csv' # Nov 2019 Municipal and Special Election
rm 'data/raw/Georgia/Bartow/Bartow CVR_Export_20220815113750.csv' # May 2020 General Primary
rm 'data/raw/Georgia/Bartow/Bartow CVR_Export_20220815115321.csv' # 2021 11 02 Municipal General M

## Georgia/Candler
echo "Georgia/Candler"
rm data/raw/Georgia/Candler/cvr2.csv

## Georgia/Whitfield
echo "Georgia/Whitfield"

rm 'data/raw/Georgia/Whitfield/Whitefield CVR_Export_20220606155917.csv' # 2022 05 24 Gen Prim
rm 'data/raw/Georgia/Whitfield/Whitefield CVR_Export_20220615110941.csv' # 2020 duplicate

## Illinois/Galena
echo "Illinois/Galena"
find data/raw/Illinois/Galena -type f ! -name "2020 GE CVR.xlsx" -exec rm -f {} \;

## Illinois/General McHenry and Illinois/McHenry
rm -rf data/raw/Illinois/General\ McHenry
rm 'data/raw/Illinois/McHenry/03Nov2020_IL_General_McHenry_Ballot Table-8-5-2022.xlsx'

## Illinois/Lake
# remove all files in Illinois/Lake that start with GE20
echo "Illinois/Lake"
find data/raw/Illinois/Lake -type f -name "GE20*" -exec rm -f {} \;

## Michigan/Eaton
echo "Michigan/Eaton"
rm data/raw/Michigan/Eaton/detail.txt
rm data/raw/Michigan/Eaton/detail.xls
rm data/raw/Michigan/Eaton/detail.xml
rm data/raw/Michigan/Eaton/summary.csv

## Michigan/Gogebic
echo "Michigan/Gogebic"
find data/raw/Michigan/Gogebic -type f -name "CVR_Export*" -exec rm -f {} \; # 2022 primary files
rm 'data/raw/Michigan/Gogebic/Conditional Votes 20220809115821.txt' # misc file

## Michigan/Manistee
echo "Michigan/Manistee"
rm -rf 'data/raw/Michigan/Manistee/CVR_Export_20220826160046'
rm -rf 'data/raw/Michigan/Manistee/__MACOSX'

head -n 1 data/raw/Michigan/Manistee/Cleaned/CvrExport_13.csv > data/raw/Michigan/Manistee/cvr.csv && tail -n+2 -q data/raw/Michigan/Manistee/Cleaned/CvrExport*.csv >> data/raw/Michigan/Manistee/cvr.csv
rm -rf data/raw/Michigan/Manistee/Cleaned

## Michigan/Schoolcraft
# actually fake
rm -rf data/raw/Michigan/Schoolcraft

## Nevada/Clark
echo "Nevada/Clark"
mv data/raw/Nevada/Clark/CLARKCVRPRESIDENT.csv data/raw/Nevada/Clark/ClarkCVRSPresident.csv
mv data/raw/Nevada/Clark/clarkCVRSAD5.csv data/raw/Nevada/Clark/ClarkCVRSAD5.csv

head -n 1 data/raw/Nevada/Clark/ClarkCRSSD11.csv > data/raw/Nevada/Clark/cvr.csv && tail -n+2 -q data/raw/Nevada/Clark/Clark*.csv >> data/raw/Nevada/Clark/cvr.csv
rm data/raw/Nevada/Clark/Clark*.csv

## New Jersey/Cumberland
echo "New Jersey/Cumberland"
head -n 1 data/raw/New\ Jersey/Cumberland/CumberlandMDCVRHouse.csv > data/raw/New\ Jersey/Cumberland/cvr.csv && tail -n+2 -q data/raw/New\ Jersey/Cumberland/Cumberland*.csv >> data/raw/New\ Jersey/Cumberland/cvr.csv
rm data/raw/New\ Jersey/Cumberland/Cumberland*.csv

## New Jersey/Salem
echo "New Jersey/Salem"
find data/raw/New\ Jersey/Salem -type f -name "*.json" -exec rm -f {} \;

## New Jersey/Temp
echo "New Jersey/Temp but actually Utah/San Juan"
mkdir data/raw/Utah
mkdir data/raw/Utah/San\ Juan
mv data/raw/New\ Jersey/Temp/* data/raw/Utah/San\ Juan/
rmdir data/raw/New\ Jersey/Temp

## Ohio/Columbiana
# no 2020 data
echo "Ohio/Columbiana"
rm -rf data/raw/Ohio/Columbiana

## Ohio/Hancock
echo "Ohio/Hancock"
find data/raw/Ohio/Hancock -type f ! -name "2020.NOV.CVR.csv" -exec rm -f {} \;

## Oregon/Columbia
echo "Oregon/Columbia"
rm data/raw/Oregon/Columbia/Cast-Vote-Record_11-2020ColumbiaOR.xlsx

## Oregon/Deschutes
echo "Oregon/Deschutes"
rm data/raw/Oregon/Deschutes/OR*.csv

## Oregon/Douglas
echo "Oregon/Douglas"
rm data/raw/Oregon/Douglas/November*.csv

## Oregon/Wasco
echo "Oregon/Wasco"
rm data/raw/Oregon/Wasco/CVR\_*.csv

## Oregon/Washington
echo "Oregon/Washington"
rm data/raw/Oregon/Washington/election*.csv

## Texas/Denton
echo "Texas/Denton"
head -n 1 data/raw/Texas/Denton/CVR\ -\ President.csv > data/raw/Texas/Denton/cvr.csv && tail -n+2 -q data/raw/Texas/Denton/CVR*.csv >> data/raw/Texas/Denton/cvr.csv
rm data/raw/Texas/Denton/CVR*.csv

## Texas/Fannin
echo "Texas/Fannin"
rm -rf data/raw/Texas/Fannin

## Texas/Montgomery
echo "Texas/Montgomery"

find data/raw/Texas/Montgomery/ -name "C0*.csv" -exec cat {} \; > data/raw/Texas/Montgomery/cvr.csv

## Texas/Nacogdoches
echo "Texas/Nacogdoches"
find data/raw/Texas/Nacogdoches -type f -name "*.zip" -execdir sh -c 'unzip -o "{}" && rm "{}"' \;

## Texas/Parker
echo "Texas/Parker"
unzip -o data/raw/Texas/Parker/CVRArchive_4997_4997_0_4088.zip -d data/raw/Texas/Parker/

# move all files in data/raw/Texas/Parker/CVR to data/raw/Texas/Parker
find data/raw/Texas/Parker/CVR -type f -exec mv {} data/raw/Texas/Parker \;
find data/raw/Texas/Parker/CVR\ 2 -type f -exec mv {} data/raw/Texas/Parker \;

rm -rf data/raw/Texas/Parker/WriteIn
rmdir data/raw/Texas/Parker/CVR
rmdir data/raw/Texas/Parker/CVR\ 2

## Texas/Polk
echo "Texas/Polk"

unzip -o data/raw/Texas/Polk/CVRArchive_648_648_3862.zip -d data/raw/Texas/Polk/
unzip -o data/raw/Texas/Polk/CVRArchive_676_676_1432.zip -d data/raw/Texas/Polk/
unzip -o data/raw/Texas/Polk/CVRArchive_800_800_4212.zip -d data/raw/Texas/Polk/

rm data/raw/Texas/Polk/CVRArchive_648_648_3862.zip
rm data/raw/Texas/Polk/CVRArchive_676_676_1432.zip
rm data/raw/Texas/Polk/CVRArchive_800_800_4212.zip

## Texas/Ward
echo "Texas/Ward"

rm -rf 'data/raw/Texas/Ward/March 2022 Primary CVRs'
unzip -o 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_6_6_0_488.zip' -d 'data/raw/Texas/Ward/Nov 2020 CVRs/'
unzip -o 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_24_24_0_0932.zip' -d 'data/raw/Texas/Ward/Nov 2020 CVRs/'
unzip -o 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_938_938_0_5952.zip' -d 'data/raw/Texas/Ward/Nov 2020 CVRs/'
unzip -o 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_3150_3150_0_9612.zip' -d 'data/raw/Texas/Ward/Nov 2020 CVRs/'

rm 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_6_6_0_488.zip'
rm 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_24_24_0_0932.zip'
rm 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_938_938_0_5952.zip'
rm 'data/raw/Texas/Ward/Nov 2020 CVRs/CVRArchive_3150_3150_0_9612.zip'

## New Jersey/Gloucester
sed 's/TOWNSHIP OF EAST GREENWICH DISTRICTS 1-5, 7/TOWNSHIP OF EAST GREENWICH DISTRICTS 1-5 7/g' 'data/raw/New Jersey/Gloucester/2020CVR.csv' > 'data/raw/New Jersey/Gloucester/cvr_modified.csv'
sed -i '' 's/, SR.,/ SR.,/g' 'data/raw/New Jersey/Gloucester/cvr_modified.csv'
sed -i '' 's/, JR.,/ JR.,/g' 'data/raw/New Jersey/Gloucester/cvr_modified.csv'
sed -i '' 's/, II,/ II,/g' 'data/raw/New Jersey/Gloucester/cvr_modified.csv'

## Arizona/Santa Cruz
sed 's/ JR.,/ JR./g' 'data/raw/Arizona/Santa Cruz/cvr.csv' > 'data/raw/Arizona/Santa Cruz/cvr_modified.csv'
sed -i '' 's/,",",/,,,/g' 'data/raw/Arizona/Santa Cruz/cvr_modified.csv'

## Minnesota/Fillmore
head -n 1 data/raw/Minnesota/Fillmore/cvr.csv > data/raw/Minnesota/Fillmore/cvr_combined.csv && tail -n+2 -q 'data/raw/Minnesota/Fillmore/2020 Fillmore County General Election Cast Vote Record #2.csv' >> data/raw/Minnesota/Fillmore/cvr_combined.csv && tail -n+2 -q 'data/raw/Minnesota/Fillmore/cvr.csv' >> data/raw/Minnesota/Fillmore/cvr_combined.csv

sed 's/write-in, if any/write-in/g' data/raw/Minnesota/Fillmore/cvr_combined.csv > data/raw/Minnesota/Fillmore/cvr_modified.csv

## Illinois/Monroe
sed -i 's/(JOSEPH R. BIDEN/JOSEPH R. BIDEN/g' data/raw/Illinois/Monroe/GE2020CVR.csv
sed -i 's/REP (DONALD J. TRUMP/REP DONALD J. TRUMP/g' data/raw/Illinois/Monroe/GE2020CVR.csv

## Illinois/Clinton
sed -i 's/(JOSEPH R. BIDEN/JOSEPH R. BIDEN/g' data/raw/Illinois/Clinton/03Nov2020_IL_General_Clinton_CVR-7-7-2022.csv
sed -i 's/REP (DONALD J. TRUMP/REP DONALD J. TRUMP/g' data/raw/Illinois/Clinton/03Nov2020_IL_General_Clinton_CVR-7-7-2022.csv

## Colorado/Dolores
# has a totals row at the end of the file that needs deleting
sed '$ d' data/raw/Colorado/Dolores/cvr.csv > data/raw/Colorado/Dolores/cvr2.csv

## California/Alameda
sed -i -E 's/1 \([0-9]+%\)/1/g; s/0 \([0-9]+%\)/0/g' data/raw/California/Alameda/cvr_headers_merged.csv

Rscript -e "targets::tar_make()"