suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(readxl)
  library(fs)
})

username <- Sys.info()["user"]
if (username == "shirokuriwaki" | str_detect(username, "^sk[0-9]+")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

PATH_interim <- path(PATH_parq, "intermediate/coalesced")
PATH_release <- path(PATH_parq, "release")

# Data ---
ds <- open_dataset(PATH_interim)

# Classifications ----
# update compare.xlsx
source("code/compare.R")
Sys.sleep(3)

use_counties <- read_excel(
  path(PATH_parq, "combined/compare.xlsx"),
  sheet = "by-county") |>
  filter(release == 1) |>
  select(state, county_name)

# counties to remove precinct info from
redact_precinct <- read_csv(
  path(PATH_parq, "intermediate/precinct_crosswalk/cvr_id_to_precinct_group.csv.gz"),
  show_col_types = FALSE
) |>
  distinct() |>
  mutate(cvr_id = as.integer(cvr_id))

# clear out release directory so we get a fresh copy everytime, if needed
# if (dir_exists(PATH_release)) dir_delete(PATH_release)

# Subset and WRITE ----
ds |>
  # limit to release counties
  semi_join(use_counties, by = c("state", "county_name")) |>
  select(-matches("contest")) |>
  # redact precincts
  left_join(redact_precinct, by = c("state", "county_name", "cvr_id", "precinct_cvr", "precinct_medsl"),
            relationship = "many-to-one") |>
  mutate(precinct_medsl = coalesce(precinct_medsl_group, precinct_medsl)) |>
  mutate(precinct_cvr   = coalesce(precinct_cvr_group, precinct_cvr)) |>
  relocate(precinct_medsl, precinct_cvr, .after = precinct) |>
  select(-precinct, -matches("precinct_.*_group")) |>
  write_dataset(
    path = PATH_release,
    partitioning = c("state", "county_name"),
    format = "parquet"
  )

gc()

