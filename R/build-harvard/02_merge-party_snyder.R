library(arrow)
library(duckplyr)
library(tidyverse)
library(fs)

# unlimited memory
# https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html
# con <- duckplyr:::get_default_duckdb_connection()
# DBI::dbExecute(con, "SET preserve_insertion_order = false;")

Sys.setenv(DUCKPLYR_FORCE = TRUE)

source("R/build-harvard/R/parse.R")

# Datasets -- metadata----
meta    <- duckplyr_df_from_parquet(path(PATH_projdir, "to-parquet", "item_choice_info/*.parquet"))
ds_orig <- duckplyr_df_from_parquet(PATH_long2) |>
  select(-line_number)
ds_prec <- duckplyr_df_from_parquet(path(PATH_prec_js, "*/*.parquet"))

# filenames
filenames <- read_csv("R/build-harvard/input_files.txt",
                      name_repair = "unique_quiet",
                      show_col_types = FALSE)$file

# Main merge ----

tictoc::tic()
walk(
  .x = rev(filenames),
  .f = function(x, dir = PATH_projdir) {
    # get state and county
    st <- as.character(parse_js_fname(x)["state"])
    ct <- as.character(parse_js_fname(x)["county"])
    cli::cli_progress_bar(format = "State {st}")

    ds <- ds_orig |>
      filter(state == st, county == ct) |>
      inner_join(
        select(meta,
               state, county, column, item, choice_id,
               dist,
               party, level,
               office_type,
               nonpartisan, unexp_term,
               incumbent, measure, num_votes),
        by = c("state", "county", "column", "item", "choice_id"),
        relationship = "many-to-one")

    # Precinct
    ds_prec_st <- ds_prec |>
      filter(state == st, county == ct)

    # Party ID ---
    # follows Snyder "MAKE PARTISANSHIP" in `analysis_all_politics_partisan.do`
    ds_pid <- ds |>
      filter(level == "N") |>
      mutate(
        D = as.integer(party == "DEM"),
        R = as.integer(party == "REP"),
      ) |>
      summarize(
        pres = if_else(
          any(D == 1 & item == "US_PRES"),  "D",
          if_else(
            any(R == 1 & item == "US_PRES"), "R",
            if_else(
              any(party == "LBT" & item == "US_PRES"), "LBT",
              NA_character_
            )
          )
        ),
        pid = if_else(
          sum(D)  > 0 & sum(R) == 0, "DEM",
          if_else(
            sum(D)  > 0 & sum(R) > 0, "SPL",
            if_else(
              sum(D) == 0 & sum(R) > 0, "REP",
              NA_character_
            )
          )
        ),
        .by = c(state, county, cvr_id)
      ) |>
      select(state, county, cvr_id, pres, pid)

    # Write ----
    ds |>
      # merge prec
      left_join(
        ds_prec_st,
        by = c("state", "county", "cvr_id"),
        relationship = "many-to-one") |>
      # merge national party
      left_join(
        ds_pid,
        by = c("state", "county", "cvr_id"),
        relationship = "one-to-one") |>
      relocate(precinct, pres, pid, .after = cvr_id) |>
      write_dataset(
        path = PATH_merged,
        format = "parquet",
        partitioning = c("state", "county"),
        existing_data_behavior = "delete_matching")
  },
  .progress = "counties"
)
tictoc::toc()
# 1hr

