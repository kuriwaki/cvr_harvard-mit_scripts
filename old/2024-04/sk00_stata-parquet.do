** Test Python --> Parquet in Stata

cd ~/Dropbox/CVR_Data_Shared/data_main

python: import pandas as pd
python: import pyarrow as pa
python: import pystata as ps
python: import os

python: out = pd.read_stata("STATA_long/CA_Inyo.long.dta")

