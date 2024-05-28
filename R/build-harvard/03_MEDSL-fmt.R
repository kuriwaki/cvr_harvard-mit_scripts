# Format to MEDSL's variables,
# limited to state legislature and up

library(tidyverse)
library(arrow)
library(duckplyr)


source("R/build-harvard/R/parse.R")
source("R/build-harvard/fmt_to_medsl.R")


# other users should make a different clause
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_merged2 = "~/Downloads/stata_long/*/*/*.parquet"
  PATH_medsl_share = "~/Dropbox/CVR_parquet/harvard"
}


# Datasets
ds <- duckplyr_df_from_parquet(PATH_merged2)
states_vec = ds |> distinct(state) |> pull(state) |> sort()


# done in about 10 min
for (st in states_vec) {
  ds |>
    filter(state == st) |>
    filter(item %in% c("US_PRES", "US_REP", "US_SEN", "US_SEN (S)", "ST_SEN", "ST_REP", "ST_GOV")) |>
    fmt_harv_to_medsl() |>
    mutate(
      county_name = case_match(
        county_name,
        "BLECKLY" ~ "BLECKLEY",
        "GUADELUPE" ~ "GUADALUPE",
        "ROCKFORD" ~ "WINNEBAGO",
        "BLOOMINGTON" ~ "MCLEAN",
        .default = county_name),
    ) |>
    group_by(state, county_name) |>
    # WRITE
    write_dataset(
      path = PATH_medsl_share,
      format = "parquet",
      existing_data_behavior = "delete_matching")
  cli::cli_alert_info("{st}")
}

