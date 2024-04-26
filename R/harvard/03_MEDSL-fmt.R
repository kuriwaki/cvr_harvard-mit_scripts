# Format to MEDSL's variables,
# then limit to state leg and up

library(tidyverse)
library(arrow)
library(duckplyr)
source("fmt_to_medsl.R")


PATH_merged2 = "~/Downloads/stata_long/*/*/*.parquet"
PATH_medsl_share = "~/Dropbox/CVR_parquet/harvard/"

# Datasets
ds <- duckplyr_df_from_parquet(PATH_merged2)
states_vec = ds |> distinct(state) |> pull(state)


# done in about 10 min
for (st in states_vec) {
  ds |>
    filter(state == st) |>
    filter(item %in% c("US_PRES", "US_REP", "US_SEN", "US_SEN (S)", "ST_SEN", "ST_REP", "ST_GOV")) |>
    fmt_harv_to_medsl() |>
    group_by(state, county_name) |>
    write_dataset(
      path = PATH_medsl_share,
      format = "parquet",
      existing_data_behavior = "delete_matching")
  cli::cli_alert_info("{st}")
}

# med <- open_dataset("~/Downloads/pass1/")
# med |> filter(county_name == "CLEAR CREEK") |> count(office, contest, sort = TRUE) |> collect()
