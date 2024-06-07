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

# classifications ----
compare <- read_excel(path(PATH_parq, "combined/county-classifications_finer.xlsx"))

use_counties <- compare |>
  filter(color2 %in% c("any < 1% mismatch", "0 difference")) |>
  select(-color2)

rm_counties <- read_csv("R/release/metadata/counties_remove.csv", col_types = "cc") |>
  mutate(county_name = replace_na(county_name, ""))


# add MEDSL data ----
open_dataset(path(PATH_parq, "medsl/")) |>
  inner_join(use_counties, by = c("state", "county_name")) |>
  write_dataset(
    path(PATH_parq, "release"),
    partitioning = c("state", "county_name"),
    format = "parquet")


# optionally, add Harvard data ----
harvard_adds <- tribble(
  ~state, ~county_name,
  "CALIFORNIA", "LOS ANGELES"
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
