library(tidyverse)
library(haven)

source("R/build-harvard/R/parse.R")
source("R/build-harvard/fmt_to_medsl.R")
meta <- read_dta("~/Dropbox/CVR_Data_Shared/data_main/item_choice_info.dta")

# to csv ----
meta |>
  filter(item %in% c("US_PRES", "US_REP", "US_SEN", "US_SEN (S)", "ST_SEN", "ST_REP", "ST_GOV")) |>
  fmt_harv_to_medsl() |>
  write_csv("R/build-harvard/metadata/contests_snyder.csv", na = "")



# Metadata file into parquet -----
read_dta(path(PATH_projdir, "item_choice_info.dta")) |>
  mutate(
    level = replace(level, level == "", "L"),
    office_type = case_when(
      level == "N" ~ "federal",
      level == "S" & (nonpartisan == 0 | is.na(nonpartisan)) & measure == 0 ~ "state partisan",
      level == "L" & (nonpartisan == 0 | is.na(nonpartisan)) & measure == 0 ~ "local partisan",
      level == "S" & (nonpartisan == 1 | nonpartisan == 2) & measure == 0 ~ "state nonpartisan",
      level == "L" & (nonpartisan == 1 | nonpartisan == 2) & measure == 0 ~ "local nonpartisan",
      level == "S" & measure == 1 ~ "state measure",
      level == "L" & measure == 1 ~ "local measure",
      retention == 1 ~ "retention",
      is.na(nonpartisan) & choice %in% c("NA", "") &
        item %in% c("NA", "") ~ "unclassified"
    ),
    .after = item
  ) |>
  write_dataset(path(PATH_projdir, "to-parquet", "item_choice_info"), format = "parquet")

