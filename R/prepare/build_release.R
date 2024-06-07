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

# classifications
compare <- read_excel(path(PATH_parq, "combined/county-classifications_finer.xlsx")) |>
  filter(color2 %in% c("any < 1% mismatch", "0 difference")) |>
  select(-color2)


# add MEDSL data ----
open_datset(path(PATH_parq, "medsl/")) |>
  inner_join(compare, by = c("state", "county_name")) |>
  write_dataset("../CVR_parquet/release", partitioning = c("state", "county_name"), format = "parquet")


# optionally, add Harvard data ----
harvard_adds <- tribble(
  ~state, ~county_name,
  "CALIFORNIA", "LOS ANGELES"
)

open_datset(path(PATH_parq, "medsl/")) |>
  inner_join(harvard_adds, by = c("state", "county_name")) |>
  write_dataset(
    "../CVR_parquet/release",
    existing_data_behavior = "delete_matching",
    partitioning = c("state", "county_name"),
    format = "parquet")
