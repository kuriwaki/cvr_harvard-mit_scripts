suppressPackageStartupMessages({
  library(arrow)
  library(tidyverse)
  library(haven)
  library(cli)
  library(tictoc)
})

tic()
snyder_long <- read_dta("~/Projects/snyder_subset-with-metadata.dta")
toc()
cli::cli_alert_info("Finished Read in data")

count(snyder_long, state, sort = TRUE)
count(snyder_long, item, sort = TRUE)

# change to character and write to parquet
snyder_long |>
  mutate(across(where(is.labelled), \(x) as.character(as_factor(x)))) |>
  fmt_harv_to_medsl() |>
  group_by(state) |>
  # Write ----
  write_dataset(
    path = "data/harvard/cvrs_harv_medsl-fmt",
    #  NOTE: cvrs_long. without the `fmt_harv_to_medsl`
    existing_data_behavior = "delete_matching")
cli::cli_alert_info("Finished writing to parquet")
