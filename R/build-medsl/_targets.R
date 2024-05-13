library(targets)
library(tarchetypes)
suppressPackageStartupMessages(library(tidyverse))

source("code/cvrs/functions.R")

raw_paths = read_csv("code/cvrs/util/paths.csv", col_select = -notes) |> 
  mutate(county_name = replace_na(county_name, ""))

options(
  readr.show_col_types = FALSE
)

tar_option_set(
  packages = c("tidyverse", "arrow", "janitor", "jsonlite", "furrr", "readxl", "xml2", "stringi", "fs"),
  memory = "transient",
  garbage_collection = TRUE,
  controller = crew::crew_controller_local(workers = 6, garbage_collection = TRUE, seconds_timeout = 120)
)

tar_config_set(
  seconds_meta_append = 15,
  seconds_reporter = 0.5
)

py_juris = tibble(
  state = c(
    "PENNSYLVANIA",
    "RHODE ISLAND", "RHODE ISLAND", "RHODE ISLAND", "RHODE ISLAND", "RHODE ISLAND", "RHODE ISLAND",
    "VIRGINIA",
    "WEST VIRGINIA", "WEST VIRGINIA"
  ),
  county_name = c(
    "ALLEGHENY",
    "", "BRISTOL", "KENT", "NEWPORT", "PROVIDENCE", "WASHINGTON",
    "",
    "NICHOLAS", "WOOD"
  )
)

## Remove any directories that might be leftover from previous spellings, or wrong counties.
stale_dirs = arrow::open_dataset("data/pass1/", format = "parquet", partitioning = c("state", "county_name")) |>
  distinct(state, county_name) |>
  collect() |>
  anti_join(bind_rows(raw_paths, py_juris), join_by(state, county_name)) |>
  mutate(
    path = str_c("data/pass1/state=", state, "/county_name=", county_name) |>
      str_replace_all(fixed(" "), fixed("%20")) |>
      str_replace_all(fixed("'"), fixed("%27"))
    )

walk(stale_dirs$path, fs::dir_delete)

## Begin `targets` pipeline
list(
  tar_map(
    filter(raw_paths, type == "xml"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass1, process_xml(path, state, county_name, contests), format = "file", error = "continue", deployment = "main"),
    names = c(state, county_name)
  ),
  tar_map(
    filter(raw_paths, type == "special"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass1, process_special(path, state, county_name, contests), format = "file", error = "continue"),
    names = c(state, county_name)
  ),
  tar_map(
    filter(raw_paths, type == "delim"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass1, process_delim(path, state, county_name, contests), format = "file", error = "continue"),
    names = c(state, county_name)
  ),
  tar_map(
    filter(raw_paths, type == "json"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass1, process_json(path, state, county_name, contests), format = "file", error = "continue", deployment = "main"),
    names = c(state, county_name)
  )
)
