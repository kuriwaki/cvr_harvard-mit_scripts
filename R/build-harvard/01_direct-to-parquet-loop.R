# Read from Snyder Dropbox
library(tidyverse)
library(haven)
library(arrow)
library(glue)
library(fs)

source("R/build-harvard/R/parse.R")

# All files -- setup -----
filenames <- read_csv("R/build-harvard/input_files.txt",
                      name_repair = "unique_quiet",
                      show_col_types = FALSE)$file

paths_to_merge <-
  c("CA_Alameda_long.dta",
    "CA_Contra_Costa_long.dta",
    "CA_Kings_long.dta",
    "CA_Merced_long.dta",
    "CA_Orange_long.dta",
    "CA_San_Benito_long.dta",
    "CA_San_Bernardino_long.dta",
    "CA_San_Francisco_long.dta",
    "CA_Santa_Clara_long.dta",
    "CA_Sonoma_long.dta",
    "CA_Ventura_long.dta",
    "CO_Denver_long.dta",
    "CO_Eagle_long.dta" ,
    "MD_Baltimore_long.dta",
    "MD_Baltimore_City_long.dta",
    "MD_Montgomery_long.dta")

# TODO: check Bernadino and San Diego, and Marin (17) -- lot of removal of duplicates, 6-19%

# Main -----
tictoc::tic()
walk(
  .x = filenames,
  .f = function(x, dir = PATH_projdir) {
    gc()
    # read
    dat <- read_dta(fs::path(dir, "STATA_long", x))

    # get state and county
    st <- as.character(parse_js_fname(x)["state"])
    ct <- as.character(parse_js_fname(x)["county"])

    # follows Snyder MERGE SOME SPLIT CVR RECORDS in `analysis_all_politics_partisan.do`
    if (x %in% paths_to_merge) {
      # update and redefine new cvr_id
      info <- glue("{dir}/STATA_cvr_info/{st}_{ct}_cvr_info.dta") |>
        read_dta() |>
        filter(!is.na(cvr_id)) |>
        select(cvr_id, cvr_id_merged)

      dat <- info |> # one row per cvr_id
        left_join(dat, by = "cvr_id", relationship = "one-to-many") |>
        mutate(cvr_id = coalesce(cvr_id_merged, cvr_id)) |>
        filter(!is.na(item) & !is.na(choice))
    }

    # Append county info and write
    dat |>
      duckplyr::as_duckplyr_df() |>
      # follows Snyder DROP SOME CASES OF DUPLICATED cvr_id in `analysis_all_politics_partisan.do`
      # (seems to remove 5% of rows in LA county for example -- maybe RCV?)
      # Stata code here is `egen x = count(cvr_id), by(cvr_id column)` followed by `drop if x > 1`. Not sure if this is the same
      tidylog::filter(n() == 1, .by = c(cvr_id, column)) |>
      # this filter cannot be down with duckdb yet
      mutate(
        state = st,
        county = ct,
        .before = 1) |>
      write_dataset(
        path = PATH_long,
        partitioning = c("state", "county"),
        format = "parquet",
        existing_data_behavior = "delete_matching")
  },
  .progress = "counties"
)
tictoc::toc() # 2 hours (I thought it was only 40 minutes without the filter)
