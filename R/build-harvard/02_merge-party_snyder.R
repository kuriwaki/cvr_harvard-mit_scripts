library(arrow)
library(duckplyr)
library(tidyverse)
library(fs)

# unlimited memory
# https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html
# con <- duckplyr:::get_default_duckdb_connection()
# DBI::dbExecute(con, "SET preserve_insertion_order = false;")

# Sys.setenv(DUCKPLYR_FORCE = TRUE)
duckplyr::methods_overwrite()

source("R/build-harvard/R/parse.R")

# Datasets -- metadata----
meta    <- duckplyr_df_from_parquet(path(PATH_projdir, "to-parquet", "item_choice_info/*.parquet"))
ds_orig <- duckplyr_df_from_parquet(PATH_long2) |>
  select(-line_number)
ds_prec <- duckplyr_df_from_parquet(path(PATH_prec_js, "*/*.parquet"))

# filenames
filenames <- read_csv("R/build-harvard/input_files.txt",
                      name_repair = "unique_quiet",
                      show_col_types = FALSE) |>
  pull(file)

# Main merge ----

tictoc::tic()
walk(
  .x = filenames,
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
      left_join(ds_prec_st,
                by = c("state", "county", "cvr_id"),
                relationship = "many-to-one") |>
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
  },
  .progress = "counties"
)
tictoc::toc()
# 13 min
# 4hr 36min all (+ one LA)

