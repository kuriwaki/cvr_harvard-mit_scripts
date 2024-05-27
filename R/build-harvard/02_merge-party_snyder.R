library(arrow)
library(duckplyr)
library(tidyverse)
library(fs)

# unlimited memory
# https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html
con <- duckplyr:::get_default_duckdb_connection()
DBI::dbExecute(con, "SET preserve_insertion_order = false;")



source("R/build-harvard/R/parse.R")

# Datasets -- metadata----
meta    <- duckplyr_df_from_parquet(path(PATH_projdir, "to-parquet", "item_choice_info/*.parquet"))
ds_orig <- duckplyr_df_from_parquet(PATH_long2) |>
  select(-line_number)
ds_prec <- duckplyr_df_from_parquet(path(PATH_prec_js, "*/*.parquet"))

# Main merge ----

# Have to do this state by state since my R crashes otherwise
states_vec <- distinct(ds_orig, state) |>
  pull(state) |>
  sort() |>
  rev()

tictoc::tic()
for (st in rev(c("CA", "CO", "FL"))) {
  gc()
  cli::cli_alert_info("{st}")

  ds <- ds_orig |>
    filter(state == st) |>
    inner_join(
      select(meta,
             state:dist, choice_id,
             party, level,
             office_type,
             nonpartisan, unexp_term,
             incumbent, spending, measure, multi_county, num_votes),
      by = c("state", "county", "column", "item", "choice_id"),
      relationship = "many-to-one")

  # Precinct
  ds_prec_st <- ds_prec |>
    filter(state == st)

  # Party ID ---
  # follows Snyder "MAKE PARTISANSHIP" in `analysis_all_politics_partisan.do`
  ds_pid <- ds |>
    filter(level == "N") |>
    mutate(
      D = as.numeric(party == "DEM"),
      R = as.numeric(party == "REP"),
    ) |>
    summarize(
      pres = case_when(
        any(D == 1 & item == "US_PRES") ~ "D",
        any(R == 1 & item == "US_PRES") ~ "R",
        any(party == "LBT" & item == "US_PRES") ~ "LBT",
        .default = NA_character_
      ),
      pid = case_when(
        sum(D)  > 0 & sum(R) == 0 ~ "DEM",
        sum(D)  > 0 & sum(R) > 0  ~ "SPL",
        sum(D) == 0 & sum(R) > 0 ~ "REP",
      ),
      .by = c(state, county, cvr_id)
    ) |>
    select(state, county, cvr_id, pid, pres)

  # Write ----
  ds |>
    # merge prec
    left_join(ds_prec_st, by = c("state", "county", "cvr_id")) |>
    # merge national party
    left_join(
      ds_pid,
      by = c("state", "county", "cvr_id"),
      relationship = "one-to-one") |>
    relocate(pres, pid, .after = cvr_id) |>
    group_by(state, county) |> # specifies partition
    write_dataset(
      path = PATH_merged,
      format = "parquet",
      existing_data_behavior = "delete_matching")
}
tictoc::toc()
# 13 min
