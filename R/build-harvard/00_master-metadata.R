library(tidyverse)
library(haven)


source("R/build-harvard/fmt_to_medsl.R")
meta <- read_dta("~/Dropbox/CVR_Data_Shared/data_main/item_choice_info.dta")

meta |>
  filter(item %in% c("US_PRES", "US_REP", "US_SEN", "US_SEN (S)", "ST_SEN", "ST_REP", "ST_GOV")) |>
  fmt_harv_to_medsl() |>
  write_csv("R/build-harvard/metadata/contests_snyder.csv", na = "")
