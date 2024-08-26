library(targets)
library(tarchetypes)
suppressPackageStartupMessages(library(tidyverse))
library(fs)

source("code/utils.R")
source("code/functions.R")
source("code/function_contests.R")

options(
  readr.show_col_types = FALSE
)

tar_option_set(
  packages = c("tidyverse", "arrow", "janitor", "jsonlite", "furrr", "readxl", "xml2", "stringi", "fs", "data.table", "dominionCVR"),
  memory = "transient",
  format = "parquet",
  garbage_collection = TRUE,
  controller = crew::crew_controller_local(
    workers = 5, garbage_collection = TRUE,
    seconds_timeout = 120, launch_max = 20,
    seconds_launch = 60
    )
)

tar_config_set(
  seconds_meta_append = 15,
  seconds_reporter = 0.5
)

raw_paths = read_csv("metadata/paths.csv", col_select = -notes) |> 
  mutate(county_name = replace_na(county_name, "STATEWIDE"))

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

# get the counties that are perfect already, no need to touch them
green_counties = readxl::read_excel("metadata/compare.xlsx", sheet = "by-county") |> 
  filter(match_score_m == 1) |>
  select(state, county_name)

# remove the counties that have persistent issues or are perfect, it's a waste of time to rerun them
raw_paths = filter(raw_paths, is.na(build)) |> 
  # anti_join(green_counties, by = c("state", "county_name")) |> 
  select(-build)

## Begin `targets` pipeline
list(
  tar_target(party_meta, get_party_meta("metadata/contest_parties.csv"), cue = tar_cue(mode = "always")),
  tar_map(
    filter(raw_paths, type == "xml"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass0, preprocess_xml(path), error = "null", deployment = "main"),
    tar_target(pass1, process_xml(pass0, state, county_name, contests), format = "file", error = "null"),
    tar_target(pass2, merge_party(party_meta, pass1, state, county_name), format = "file", error = "null", cue = tar_cue(mode = "always")),
    names = c(state, county_name)
  ),
  tar_map(
    filter(raw_paths, type == "special"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass1, process_special(path, state, county_name, contests), format = "file", error = "null"),
    tar_target(pass2, merge_party(party_meta, pass1, state, county_name), format = "file", error = "null", cue = tar_cue(mode = "always")),
    names = c(state, county_name)
  ),
  tar_map(
    filter(raw_paths, type == "delim"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass1, process_delim(path, state, county_name, contests), format = "file", error = "null"),
    tar_target(pass2, merge_party(party_meta, pass1, state, county_name), format = "file", error = "null", cue = tar_cue(mode = "always")),
    names = c(state, county_name)
  ),
  tar_map(
    filter(raw_paths, type == "json"),
    tar_target(contests, get_contests(state, county_name), cue = tar_cue(mode = "always")),
    tar_target(pass0, preprocess_json(path), error = "null", deployment = "main"),
    tar_target(pass1, process_json(pass0, state, county_name, contests), format = "file", error = "null"),
    tar_target(pass2, merge_party(party_meta, pass1, state, county_name), format = "file", error = "null", cue = tar_cue(mode = "always")),
    names = c(state, county_name)
  )
)
