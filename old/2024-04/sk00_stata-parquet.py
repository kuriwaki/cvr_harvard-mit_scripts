import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Goal: read in a large stata file, (do merges), then write to a parquet 
out = pd.read_stata('~/Desktop/CA_Inyo_long.dta') # only works for Desktop file
pq.write_table(out, '~/Desktop/example.parquet')



# pystata works as below, 
# but this just allows me to run Stata in Python -- not very useful for me
import stata_setup
stata_setup.config('/Applications/Stata', 'se')
import pystata as ps

