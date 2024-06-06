rm(list=ls())
gc()

library(tidyverse)
library(arrow)

compare = readxl::read_excel("../CVR_parquet/combined/county-classifications_finer.xlsx") |>
  filter(color2 %in% c("any < 1% mismatch", "0 difference")) |>
  select(-color2)

# add MEDSL data
open_datset("../CVR_parquet/medsl/") |>
  inner_join(compare, by = c("state", "county_name")) |>
  write_dataset("../CVR_parquet/release", partitioning = c("state", "county_name"), format = "parquet")

harvard_adds <- tribble(
  ~state, ~county_name,
  "CALIFORNIA", "LOS ANGELES"
)

# optionally, add Harvard data
open_datset("../CVR_parquet/harvard/") |>
  inner_join(harvard_adds, by = c("state", "county_name")) |>
  write_dataset(
    "../CVR_parquet/release",
    existing_data_behavior = "delete_matching",
    partitioning = c("state", "county_name"),
    format = "parquet")
