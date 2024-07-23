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
source("code/02_combine-to-wide.R")
Sys.sleep(3)

use_counties <- read_excel(
  path(PATH_parq, "combined/compare.xlsx"),
  sheet = "by-county") |>
  filter(release == 1) |>
  select(state, county_name)

# counties to remove precinct info from
redact_precinct <- read_csv(path(PATH_parq, "intermediate/precinct_crosswalk/affected_county.csv"),
                            show_col_types = FALSE)


# first, clear out release directory so we get a fresh copy everytime

if (dir_exists(PATH_release)) dir_delete(PATH_release)

# Subset and WRITE ----
ds |>
  # limit to release counties
  semi_join(use_counties, by = c("state", "county_name")) |>
  select(-matches("contest")) |>
  # redact precincts
  left_join(redact_precinct, by = c("state", "county_name")) |>
  mutate(precinct_medsl = ifelse(redact == 1 & !is.na(redact), NA, precinct_medsl)) |>
  mutate(precinct_cvr   = ifelse(redact == 1 & !is.na(redact), NA, precinct_cvr)) |>
  relocate(precinct_medsl, precinct_cvr, .after = precinct) |>
  select(-precinct, -matches("revealed_in_"), -matches("redact"), -matches("precs_revel")) |>
  write_dataset(
    path = PATH_release,
    partitioning = c("state", "county_name"),
    format = "parquet"
  )

gc()

