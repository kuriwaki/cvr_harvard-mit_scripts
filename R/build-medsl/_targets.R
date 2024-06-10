library(targets)
library(tarchetypes)
suppressPackageStartupMessages(library(tidyverse))
library(fs)

if (Sys.info()["user"] == "mason") {
  BASE_PATH = "code/"
} else {
  BASE_PATH = "code/cvrs"
}

source(path(BASE_PATH, "utils.R"))
source(path(BASE_PATH, "function_contests.R"))
source(path(BASE_PATH, "functions.R"))

options(
  readr.show_col_types = FALSE
)

tar_option_set(
  packages = c("tidyverse", "arrow", "janitor", "jsonlite", "furrr", "readxl", "xml2", "stringi", "fs", "data.table"),
  memory = "transient",
  format = "parquet",
  garbage_collection = TRUE,
  controller = crew::crew_controller_local(
    workers = 6, garbage_collection = TRUE, 
    seconds_timeout = 120, launch_max = 15,
    seconds_launch = 60
    )
)

tar_config_set(
  seconds_meta_append = 15,
  seconds_reporter = 0.5
)

raw_paths = read_csv("metadata/paths.csv", col_select = -notes) |> 
  mutate(county_name = replace_na(county_name, ""))

## Remove any directories that might be leftover from previous spellings, or wrong counties.
stale_dirs = arrow::open_dataset("data/pass1/", format = "parquet", partitioning = c("state", "county_name")) |>
  distinct(state, county_name) |>
  collect() |>
  anti_join(raw_paths, join_by(state, county_name)) |>
  mutate(
    path = str_c("data/pass1/state=", state, "/county_name=", county_name) |>
      str_replace_all(fixed(" "), fixed("%20")) |>
      str_replace_all(fixed("'"), fixed("%27"))
    )

walk(stale_dirs$path, dir_delete)

# remove the counties that have persistent issues, it's a waste of time to fix them
raw_paths = filter(raw_paths, is.na(build)) |> select(-build)

## Begin `targets` pipeline
list(
  tar_map(
    filter(raw_paths, type == "xml"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass0, preprocess_xml(path), error = "continue", deployment = "main"),
    tar_target(pass1, process_xml(pass0, state, county_name, contests), format = "file", error = "continue"),
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
    tar_target(pass0, preprocess_json(path), error = "continue", deployment = "main"),
    tar_target(pass1, process_json(pass0, state, county_name, contests), format = "file", error = "continue"),
    names = c(state, county_name)
  )
)
