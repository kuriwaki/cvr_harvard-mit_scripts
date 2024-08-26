# Read from Snyder Dropbox
library(tidyverse)
library(haven)
library(arrow)
library(duckplyr)
library(glue)
library(fs)
library(DBI)

source("R/parse.R")

# All files -- setup -----
filenames <- read_csv("metadata/input_files.txt",
                      name_repair = "unique_quiet",
                      show_col_types = FALSE)$file

# check if missing new counties
db_files <- dir_ls("~/Dropbox/CVR_Data_Shared/data_main/STATA_long") |>
  path_file()
setdiff(db_files, filenames)

paths_to_merge <-
  c("CA_Alameda_long.dta",
    "CA_Contra_Costa_long.dta",
    "CA_Glenn_long.dta",
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

Sys.setenv(DUCKPLYR_FALLBACK_AUTOUPLOAD = 1)
Sys.setenv(DUCKPLYR_FORCE = TRUE)
# TODO: check Bernadino and San Diego, and Marin (17) -- lot of removal of duplicates, 6-19%
con <- duckplyr:::get_default_duckdb_connection()
dbExecute(con, "SET preserve_insertion_order = false;")
dbExecute(con, "SET memory_limit ='20GB';")


# Main -----
tictoc::tic()
walk(
  .x = filenames,
  .f = function(x, dir = PATH_projdir) {
    gc()
    # read
    # x_csv <- str_replace(x, "\\.dta", ".csv")
    # dat <- df_from_file(
    #   path("~/Downloads/stata_csv/", x_csv),
    #   "read_csv",
    #   options = list(col_types = "dccddddd"))
    dat <- read_dta(fs::path(dir, "STATA_long", x)) |>
      zap_label() |> zap_formats() |>
      duckplyr::as_duckplyr_df()

    # get state and county
    st <- as.character(parse_js_fname(x)["state"])
    ct <- as.character(parse_js_fname(x)["county"])

    # follows Snyder MERGE SOME SPLIT CVR RECORDS in `analysis_all_politics_partisan.do`
    if (x %in% paths_to_merge) {
      # update and redefine new cvr_id
      info <- glue("{dir}/STATA_cvr_info/{st}_{ct}_cvr_info.dta") |>
        read_dta() |>
        filter(!is.na(cvr_id)) |>
        distinct(cvr_id, cvr_id_merged) # throws error in S Bernardino

      dat <- info |> # one row per cvr_id
        left_join(dat, by = "cvr_id", relationship = "one-to-many") |>
        mutate(cvr_id = coalesce(cvr_id_merged, cvr_id)) |>
        filter(!is.na(item) & !is.na(choice)) |>
        zap_label() |> zap_formats() |>
        duckplyr::as_duckplyr_df()
    }

    # Append county info and write
    # follows Snyder DROP SOME CASES OF DUPLICATED cvr_id in `analysis_all_politics_partisan.do`
    # (seems to remove 5% of rows in LA county for example -- maybe RCV?)
    # Stata code here is `egen x = count(cvr_id), by(cvr_id column)` followed by `drop if x > 1`. Not sure if this is the same
    singletons <- dat |> count(cvr_id, column) |> filter(n == 1)

    dat |>
      duckplyr::semi_join(singletons) |>
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
tictoc::toc() # 70min in dta; 40 minutes after Los Angeles
