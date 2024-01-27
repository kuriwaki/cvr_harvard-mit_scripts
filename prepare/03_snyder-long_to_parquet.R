library(arrow)
library(tidyverse)
library(haven)

snyder_long <- read_dta("~/Projects/snyder_subset-with-metadata.dta")

# change to character and write to parquet
snyder_long |>
  mutate(across(where(is.labelled), \(x) as.character(as_factor(x)))) |>
  group_by(state, item) |>
  write_dataset(
    "data/harvard/cvrs_long",
    partitioning = c("state", "item"),
    existing_data_behavior = "delete_matching")
