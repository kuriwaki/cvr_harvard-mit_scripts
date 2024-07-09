suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(fs)
})

username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

PATH_release <- path(PATH_parq, "release")

# check that it runs together ----
c_release <- open_dataset(PATH_release) |>
  distinct(state, county_name) |>
  collect()

c_release |>
  arrange(state, county_name) |>
  kableExtra::kbl(format = "pipe") |>
  write_lines("../../status/counties-in-release.txt")

c_release |>
  count(state) |>
  collect()

open_dataset(PATH_release) |>
  count(party) |>
  collect()

# check for duplicates
for (j in c("US PRESIDENT", "GOVERNOR", "US HOUSE",
            "US SENATE", "STATE SENATE", "STATE HOUSE")) {

  ds_j <- open_dataset(PATH_release) |> filter(office == j)

  if (j == "US SENATE") {
    count_j <- bind_rows(
      ds_j |> filter(state != "GEORGIA") |> count(state, county_name, cvr_id) |> collect(),
      ds_j |> filter(state == "GEORGIA") |> count(state, county_name, cvr_id, district) |> collect()
    )
  }

  if (j == "STATE HOUSE") {
    count_j <- ds_j |>
      filter(!state %in% c("WEST VIRGINIA", "ARIZONA")) |>
      count(state, county_name, cvr_id)
  }

  if (j %in% c("US HOUSE", "STATE SENATE", "US PRESIDENT", "GOVERNOR")) {
    count_j <- ds_j |> count(state, county_name, cvr_id)
  }

  dup_count <- count_j |>
    count(n, name = "nn") |>
    collect()

  stopifnot(nrow(dup_count) == 1)
  cli::cli_alert_success("{j} duplicate checked")
  gc()
}
