# Read from Snyder Dropbox
library(tidyverse)
library(purrr)
library(haven)
library(arrow)
library(glue)
library(fs)


#' changes CA_Orange_long.dta to c("CA", "Orange")
parse_js_fname <- function(chr) {
  st <- str_sub(chr, 1, 2)
  ct <- str_extract(chr, "(?<=[A-Z][A-Z]_).*(?=_long.dta)")
  c(state = st, county = ct)
}

# Output locations ------
username <- Sys.info()["user"]

# other users should make a different clause
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_projdir <- "~/Dropbox/CVR_Data_Shared/data_main"
  # Save to local tempfile. Hardcoding this path because
  # this repo is on Dropbox and I don't want to save a huge file to Dropbox
  PATH_long <- "~/Downloads/stata_init"
}


# Metadata file into parquet -----
read_dta(path(PATH_projdir, "item_choice_info.dta")) |>
  write_dataset(path(PATH_projdir, "to-parquet", "item_choice_info"), format = "parquet")

read_dta(path(PATH_projdir, "../analysis_all_politics_partisan/tmp0.dta")) |>
  write_dataset(path(PATH_projdir, "to-parquet", "item_info_tmp0"), format = "parquet")


# All files -- setup -----
filenames <- read_csv(path(PATH_projdir, "to-parquet", "input_files.txt")) |>
  pull(file)

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

# Main -----
tictoc::tic()
walk(
  .x = filenames,
  .f = function(x, dir = PATH_projdir) {
    dat <- read_dta(fs::path(dir, "STATA_long", x))

    # get state and county
    st <- parse_js_fname(x)["state"]
    ct <- parse_js_fname(x)["county"]

    # follows Snyder MERGE SOME SPLIT CVR RECORDS in `analysis_all_politics_partisan.do`
    if (x %in% paths_to_merge) {
      # update and redefine new cvr_id
      dat <- glue("{dir}/STATA_cvr_info/{st}_{ct}_cvr_info.dta") |>
        read_dta() |>
        filter(!is.na(cvr_id)) |>
        select(cvr_id, cvr_id_merged) |> # one row per cvr_id
        left_join(dat, by = "cvr_id", relationship = "one-to-many") |>
        mutate(cvr_id = coalesce(cvr_id_merged, cvr_id)) |>
        tidylog::filter(!is.na(item) & !is.na(choice))
    }

    # Append county info and write
    dat |>
      # follows Snyder DROP SOME CASES OF DUPLICATED cvr_id in `analysis_all_politics_partisan.do`
      # (seems to remove 5% of rows in LA county for example -- maybe RCV?)
      # Stata code here is `egen x = count(cvr_id), by(cvr_id column)` followed by `drop if x > 1`. Not sure if this is the same
      tidylog::filter(n() == 1, .by = c(cvr_id, column)) |>
      mutate(
        state = st,
        county = ct,
        .before = 1) |>
      group_by(state, county) |>
      write_dataset(
        path = PATH_long,
        format = "parquet",
        existing_data_behavior = "delete_matching")
  },
  .progress = "counties"
)
tictoc::toc() # 2 hours (I thought it was only 40 minutes without the filter)
