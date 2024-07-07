library(tidyverse)
library(arrow)
library(fs)

gc()

username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

PATH_release <- path(PATH_parq, "release")

# check that it runs together ----
open_dataset(PATH_release) |>
  distinct(state, county_name) |>
  count(state) |>
  collect()

open_dataset(PATH_release) |>
  count(party) |>
  collect()

# check for duplicates
for (j in c("US PRESIDENT", "GOVERNOR", "US HOUSE", "US SENATE",
            "STATE SENATE")) {

  dups_count <- open_dataset(PATH_release) |>
    filter(office == j) |>
    count(state, county_name, cvr_id, district) |>
    count(n, name = "nn") |>
    collect()
  gc()

  stopifnot(nrow(dups_count) == 1)
  cat(j, "checked\n")
}
