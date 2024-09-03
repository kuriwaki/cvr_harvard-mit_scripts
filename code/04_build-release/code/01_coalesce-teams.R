suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(fs)
})

source("code/custom-reallocate-precinct.R")
source("code/custom_add-party-metadata.R")
source("code/fmt_release.R")

gc()

username <- Sys.info()["user"]
if (username == "shirokuriwaki" | str_detect(username, "^sk[0-9]+")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

## destination for coalesced data
PATH_interim <- path(PATH_parq, "intermediate/coalesced")
PATH_precincts <- path(PATH_parq, "intermediate/precinct_crosswalk/cvr_mdsl_precinct_crosswalk")

# Precincts ----
prec_names <- open_dataset(PATH_precincts) |>
  select(-discrepancy)

# Manual counties ----
## explicitly remove the following counties
rm_counties <- read_csv("metadata/counties_remove.csv", col_types = "cc") |>
  mutate(county_name = replace_na(county_name, ""))

## swap these out of MIT if available and add to Harvad
hv_counties <- read_csv("metadata/counties_harv.csv", col_types = "cc")


# Data ----
ds_meds <- open_dataset(path(PATH_parq, "medsl/"))
ds_harv <- open_dataset(path(PATH_parq, "harvard/"))
ds_harv_sel <- ds_harv |> semi_join(hv_counties, by = c("state", "county_name"))

# temp Fixes
# https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/308#issuecomment-2212066285
seneca_rm <- ds_meds |>
  filter(state == "OHIO", county_name == "SENECA", office == "STATE HOUSE") |>
  collect() |>
  filter(n() == 2, .by = cvr_id) |>
  filter(district == "087")

## remove from MEDSL
ds_meds |>
  anti_join(rm_counties, by = c("state", "county_name")) |>
  anti_join(hv_counties, by = c("state", "county_name")) |>
  anti_join(seneca_rm, by = c("state", "county_name", "cvr_id", "office", "district")) |>
  reallocate_wi_prec() |>
  custom_add_party() |>
  fmt_for_release() |>
  fill_magnitude() |>
  left_join(prec_names, by = c("state", "county_name", "precinct"), relationship = "many-to-one") |>
  write_dataset(
    path = PATH_interim,
    existing_data_behavior = "overwrite",
    partitioning = c("state", "county_name"),
    format = "parquet")

## add Harvard
ds_harv_sel |>
  fmt_for_release() |>
  fill_magnitude() |>
  left_join(prec_names, by = c("state", "county_name", "precinct"), relationship = "many-to-one") |>
  write_dataset(
    path = PATH_interim,
    existing_data_behavior = "delete_matching",
    partitioning = c("state", "county_name"),
    format = "parquet")
