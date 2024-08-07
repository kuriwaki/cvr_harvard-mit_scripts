# Format to MEDSL's variables,
# limited to state legislature and up
library(tidyverse)
library(arrow)
library(duckplyr)


source("R/parse.R")
source("R/fmt_to_medsl.R")


# other users should make a different clause
if (username == "shirokuriwaki" | str_detect(username, "^sk[0-9]+")) {
  PATH_merged2 = "~/Downloads/stata_long/*/*/*.parquet"
  PATH_medsl_share = "~/Dropbox/CVR_parquet/harvard"
}

Sys.setenv(DUCKPLYR_FORCE = FALSE)


# Datasets
ds <- duckplyr_df_from_parquet(PATH_merged2)
states_vec = ds |> distinct(state) |> pull(state) |> sort()


tictoc::tic()
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
    write_dataset(
      path = PATH_medsl_share,
      partitioning = c("state", "county_name"),
      format = "parquet",
      existing_data_behavior = "delete_matching")
  cli::cli_alert_info("{st}")
}
tictoc::toc()
# 12 min
