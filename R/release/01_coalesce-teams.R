library(tidyverse)
library(arrow)
library(fs)

gc()

username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

## destination for coalesced data
PATH_interim <- path(PATH_parq, "intermediate/coalesced")

# Manual counties ----
## explicitly remove the following counties
rm_counties <- read_csv("R/release/metadata/counties_remove.csv", col_types = "cc") |>
  mutate(county_name = replace_na(county_name, ""))

## swap these out of MIT if available and add to Harvad
hv_counties <- read_csv("R/release/metadata/counties_harv.csv")


# Data ----
ds_meds <- open_dataset(path(PATH_parq, "medsl/"))
ds_harv <- open_dataset(path(PATH_parq, "harvard/"))
ds_harv_sel <- ds_harv |> semi_join(hv_counties, by = c("state", "county_name"))

## remove from MEDSL
ds_meds |>
  anti_join(rm_counties, by = c("state", "county_name")) |>
  anti_join(hv_counties, by = c("state", "county_name")) |>
  # Patch for misallocated precincts
  mutate(
    county_name = ifelse(county_name == "BROWN" & precinct == "Village of Pulaski W4, 7", "PULASKI", county_name),
    county_name = ifelse(county_name == "BROWN" & precinct == "Village of Wrightstown W4", "OUTAGAMIE", county_name),
    county_name = ifelse(county_name == "DANE" & precinct %in% c("V Brooklyn Wd 2", "V Belleville Wd 3"), "GREEN", county_name),
    county_name = ifelse(county_name == "DANE" & precinct == "V Cambridge Wd 1", "JEFFERSON", county_name),
    county_name = ifelse(county_name == "PIERCE" & precinct %in% c(
      "Village of Spring Valley, Ward 3",
      "City of River Falls, Wards 1-4,15"
    ), "ST CROIX", county_name),
    jurisdiction_name = ifelse(state == "WISCONSIN", county_name, jurisdiction_name)
  ) |>
  write_dataset(
    path = PATH_interim,
    existing_data_behavior = "overwrite",
    partitioning = c("state", "county_name"),
    format = "parquet")

## add Harvard
ds_harv_sel |>
  write_dataset(
    path = PATH_interim,
    existing_data_behavior = "delete_matching",
    partitioning = c("state", "county_name"),
    format = "parquet")
