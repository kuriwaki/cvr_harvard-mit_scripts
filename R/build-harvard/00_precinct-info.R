library(tidyverse)
library(haven)
library(fs)

source("R/build-harvard/R/parse.R")

# Paths ----

# Jim's file
ds_prec <- read_dta(path(PATH_projdir, "tmp_precincts_long.dta"))
ds_prec |>
  group_by(state) |>
  arrow::write_dataset(
    path = PATH_prec_js,
    format = "parquet",
    existing_data_behavior = "delete_matching")


