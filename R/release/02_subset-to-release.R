library(tidyverse)
library(arrow)
library(readxl)
library(fs)

gc()

username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

PATH_interim <- path(PATH_parq, "intermediate/coalesced")
PATH_precincts <- path(PATH_parq, "intermediate/precinct_crosswalk/cvr_mdsl_precinct_crosswalk")
PATH_release <- path(PATH_parq, "release")

# Data ---
ds <- open_dataset(PATH_interim)


# Classifications ----
# update compare.xlsx
source("R/combine/02_combine-to-wide.R")

use_counties <- read_excel(
  path(PATH_parq, "combined/compare.xlsx"),
  sheet = "by-county") |>
  filter(release == 1) |>
  select(state, county_name)

# Precincts ----
prec_names <- open_dataset(PATH_precincts) |>
  # filter(discrepancy == 0) |>
  select(-discrepancy)

# Subset and WRITE ----
ds |>
  inner_join(use_counties, by = c("state", "county_name"), relationship = "many-to-one") |>
  left_join(prec_names, by = c("state", "county_name", "precinct"), relationship = "many-to-one") |>
  select(-matches("contest")) |>
  relocate(precinct_medsl, precinct_cvr, .after = precinct) |>
  select(-precinct) |>
  write_dataset(
    path = PATH_release,
    existing_data_behavior = "overwrite",
    partitioning = c("state", "county_name"),
    format = "parquet"
  )


