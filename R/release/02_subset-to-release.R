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
PATH_release <- path(PATH_parq, "release")

# Data ---
ds <- open_dataset(PATH_interim)


# Modification functions ---
## potentially add a simple DEM/REP designation

# update compare.xlsx
source("R/combine/02_combine-to-wide.R")

# Classifications ----
use_counties <- read_excel(
  path(PATH_parq, "combined/compare.xlsx"),
  sheet = "by-county") |>
  filter(release == 1) |>
  select(state, county_name)

# Subset and WRITE ----
ds |>
  inner_join(use_counties, by = c("state", "county_name")) |>
  write_dataset(
    path = PATH_release,
    existing_data_behavior = "delete_matching",
    partitioning = c("state", "county_name"),
    format = "parquet"
  )

# check that it runs together ----
open_dataset(PATH_release) |>
  distinct(state, county_name) |>
  count(state) |>
  collect()
