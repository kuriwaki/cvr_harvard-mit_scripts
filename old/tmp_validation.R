library(tidyverse)
library(dataverse)
library(haven)

item_info <- read_dta("~/Dropbox/CVR_Data_Shared/data_main/item_choice_info.dta")

# CNN
cnn_val <- read_csv("data/val/pres2020gen_county_CNN.csv.gz")
# MEDSL
medsl_val_raw <- get_dataframe_by_name(
  filename = "countypres_2000-2020.tab",
  dataset = "10.7910/DVN/VOQCHQ",
  server = "dataverse.harvard.edu",
  original = TRUE,
  .f = read_csv
) |>
  filter(year == 2020)

# San Diego, CA
sandieg <- read_dta("~/Dropbox/CVR_Data_Shared/data_main/STATA_long/CA_San_Diego_long.dta")

sandieg |>
  filter(item == "US_PRES") |>
  count(column, choice)

item_info |>
  filter(county == "San_Diego", item == "US_PRES") |>
  count(column, choice, choice_id, party)



# Orange, TX

# Pueblo, CO

# Ouray, CO

# Tuscarawas, OH
tusc <- read_dta("~/Dropbox/CVR_Data_Shared/data_main/STATA_long/OH_Tuscarawas_long.dta")

## Biden votes coded LBT
item_info |>
  filter(county == "Tuscarawas", item == "US_PRES") |>
  count(column, choice, party)

tusc |>
  filter(item == "US_PRES") |>
  count(column, choice)

# Small errors ---


# Early, GA

# Holmes, FL

# Hockley, TX
