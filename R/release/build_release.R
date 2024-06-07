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

# Modification functions ---
## potentially add a simple DEM/REP designation


# Classifications ----
compare <- read_excel(path(PATH_parq, "combined/compare.xlsx"),
                      sheet = "by-county")

## only use these counties + those enumerated in use_harvard
use_counties <- compare |>
  filter(color2_m %in% c("any < 1% mismatch", "0 difference")) |>
  select(state, county_name)

## explicitly remove the following counties
rm_counties <- read_csv("R/release/metadata/counties_remove.csv", col_types = "cc") |>
  mutate(county_name = replace_na(county_name, ""))


# Subset MEDSL data ----
open_dataset(path(PATH_parq, "medsl/")) |>
  inner_join(use_counties, by = c("state", "county_name")) |>
  write_dataset(
    path(PATH_parq, "release"),
    existing_data_behavior = "overwrite",
    partitioning = c("state", "county_name"),
    format = "parquet")


# optionally, add Harvard data ----
harvard_adds <- tribble(
  ~state, ~county_name,
  "CALIFORNIA", "LOS ANGELES",
  "TEXAS", "LEE",
)

open_dataset(path(PATH_parq, "harvard/")) |>
  inner_join(harvard_adds, by = c("state", "county_name")) |>
  write_dataset(
    path(PATH_parq, "release"),
    existing_data_behavior = "delete_matching",
    partitioning = c("state", "county_name"),
    format = "parquet")


# check ----
open_dataset(path(PATH_parq, "release")) |>
  distinct(state, county_name) |>
  count(state) |>
  collect()
