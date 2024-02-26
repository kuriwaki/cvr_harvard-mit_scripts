# Load packages required to define the pipeline:
library(targets)

# Set target options:
tar_option_set(
  # packages that your targets need to run
  packages = c(
    "tibble",
    "arrow",
    "haven",
    "dplyr",
    "tidyr",
    "purrr",
    "readr",
    "stringr",
    "glue",
    "fs"
  ),
  format = "rds"
)

# R/ functions
tar_source()

list(
  tar_target(
    countycode,
    read_csv("data/countycodes.csv")),

  tar_target(
    parq_orig,
    open_dataset("data/MEDSL/cvrs_statewide/")
  ),

  # tar_target(
  #   distlist_CD,
  #   parq_orig |>
  #     filter(office == "US HOUSE") |>
  #     count(state, district, name = "N_voters") |>
  #     collect()
  # )
  # Snyder -----
  # party affiliation
  tar_target(snyder_cand_path, "~/Dropbox/CVR_Data_Shared/data_main/item_choice_info.dta", format = "file"),
  tar_target(
    snyder_cand,
    {
      read_dta(snyder_cand_path) |>
        select(state, county, contest = item, column, choice_id, dist, party)
    }
  ),

  tar_target(
    name = counts_snyder,
    {
      paths_vec <- read_csv("data/paths_harvard.csv") |>
        filter(dataset_format == "snyder_standard") |>
        pull(path)

      names(paths_vec) <- path_file(paths_vec)

      map(
        paths_vec,
        \(x) {
          read_dta(x) |>
            rename(any_of(c(candidate = "choice", contest = "item"))) |>
            tidylog::filter(str_detect(contest, "^(US|ST)_(PRES|REP|SEN)$")) |>
            count(contest, column, choice_id, candidate)
        },
        .progress = TRUE) |>
        list_rbind(names_to = "filename") |>
        # list state + county
        separate_wider_delim(
          filename,
          delim = "_",
          too_many = "merge",
          names = c("state", "county")) |>
        # get state and county as displayed in Jim's data, then merge to get party
        mutate(county = str_remove(county, "_long.dta")) |>
        tidylog::left_join(snyder_cand, by = c("state", "county", "column", "contest", "choice_id")) |>
        select(st = state, county_name = county, contest, dist, candidate, choice_id, party, n)
    }
  ),
  tar_target(
    count_snyder_out,
    {
      counts_snyder |>
        mutate(county_name = std_county_name(county_name)) |>
        left_join(countycode, by = c("st", "county_name")) |>
        relocate(st, state, county_name, county_fips) |>
        write_csv("data/cand-counts_harvard.csv")
    }
  )
)
