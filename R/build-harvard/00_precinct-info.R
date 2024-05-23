library(tidyverse)
library(haven)
library(fs)

source("R/build-harvard/R/parse.R")

# Paths ----
filenames <- read_csv("R/build-harvard/input_files.txt", show_col_types = FALSE) |>
  pull(file)

infonames <- filenames |>
  str_replace("_long\\.", "_cvr_info.") |>
  str_replace("STATA_long", "STATA_cvr_info")
infonames <- infonames[file_exists(path(PATH_projdir, "STATA_cvr_info", infonames))]

custom_cvr_info_field_mappings <- read_csv(
  path(PATH_jbldir, "csv/custom_cvr_info_field_mappings.csv"),
  show_col_types = FALSE)


tictoc::tic()
cvr_info_meta <- map_dfr(
  .x = infonames,
  .f = function(.x, dir = PATH_projdir) {

    # get state and county
    st <- parse_js_fname(.x)["state"]
    ct <- parse_js_fname(.x)["county"]

    dat <- read_dta(fs::path(dir, "STATA_cvr_info", .x), n_max = 10) |>
      mutate(across(everything(), as.character)) |>
      pivot_longer(
        everything(),
        names_to = "variable",
        values_to = "value") |>
      mutate(
        state = st,
        county = ct,
        .before = 1) |>
      summarize(
        values = paste(unique(value), collapse = "|"),
        .by = c("state", "county", "variable")) |>
      mutate(new_name = case_when(
        variable %in% c("cvr_id", "cvr_id_merged") ~
          as.character(variable),
        match_field_name(values, "[Pp]recinct[IDid]*") ~
          "precinct",
        match_field_name(values, "[Pp]recinct\\s*[Pp]ortion") ~
          "precinct_portion",
        match_field_name(values, "PrecinctStyleName") ~
          "precinct_portion",
        match_field_name(values, "[Bb]atch\\s*[Ii][dD]") ~
          "batch_id",
        match_field_name(values, "[Bb]allot\\s*[Tt]ype") ~
          "ballot_type",
        match_field_name(values, "[Cc]ounting\\s*[Gg]roup") ~
          "counting_group",
        match_field_name(values, "[Tt]abulator[Nn]um") ~
          "tabulator_number",
        match_field_name(values, "[Ii]mprinted[Ii]d") ~
          "imprinted",
        match_field_name(values, "PRECINCT") ~
          "precinct",
        TRUE ~ NA_character_
      ),
      .before = 4
      ) |>
      left_join(custom_cvr_info_field_mappings,
                by = c("state", "county", "variable")) |>
      mutate(new_name = coalesce(new_name.x, new_name.y)) |>
      select(-new_name.x, -new_name.y)
  })
tictoc::toc()


# about 11 min
tictoc::tic()
prec_info <- walk(
  .x = infonames,
  .f = function(.x, dir = PATH_projdir) {
    cli::cli_alert_info("{.x}")
    # get state and county
    st <- parse_js_fname(.x)["state"]
    ct <- parse_js_fname(.x)["county"]

    # metdata
    sel <- cvr_info_meta |>
      filter(state == st,
             county == ct,
             !is.na(new_name),
             new_name %in% c("precinct",
                             "precinct_portion",
                             "ballot_type")) |>
      select(-state, -county, -values)

    # read entire
    haven::read_dta(fs::path(dir, "STATA_cvr_info", .x)) |>
      filter(!is.na(cvr_id)) |>
      mutate(across(everything(), as.character)) |>
      pivot_longer(c(everything(), -cvr_id),
                   names_to = "variable",
                   values_to = "value") |>
      right_join(sel, by = "variable", relationship = "many-to-one") |>
      select(-variable) |>
      summarize(value = paste0(value, collapse = "|"),
                .by = c("cvr_id", "new_name")) |>
      pivot_wider(id_cols = cvr_id,
                  names_from = new_name,
                  values_from = value) |>
      add_cols("cvr_id_merged") |>
      add_cols("precinct") |>
      add_cols("precinct_portion") |>
      add_cols("ballot_type") |>
      add_cols("counting_group") |>
      mutate(cvr_id = as.double(coalesce(cvr_id_merged, cvr_id)),
             cvr_id_merged = as.double(cvr_id_merged)) |>
      mutate(state = st, county = ct, .before = 1) |>
      # Write to parquet
      group_by(state, county) |>
      arrow::write_dataset(
        path = PATH_prec,
        format = "parquet",
        existing_data_behavior = "delete_matching")
  },
  .progress = "counties"
)
tictoc::toc()
