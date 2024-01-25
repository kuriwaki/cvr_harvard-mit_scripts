library(tidyverse)
library(gt)
library(sf)

source("R/sum_cand-counts.R")

# Data -----
val_ush <- read_csv("data/val/ush2020_by-cd.csv")

harv_counts_raw <- read_csv("data/cand-counts_harvard.csv")

# Merge ----
comp_tbl <- harv_counts |>
  sum_ushou() |>
  mutate(
    shareD_harv = N_DEM/(N_DEM + N_REP),
    N_harv = N_REP + N_DEM,
  ) |>
  left_join(val_ush)


write_csv(comp_tbl, "data/tmp_ushou_harv.csv")
