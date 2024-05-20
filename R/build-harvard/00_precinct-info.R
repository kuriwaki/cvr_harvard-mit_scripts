library(tidyverse)
library(haven)
library(fs)

# other users should make a different clause
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_projdir <- "~/Dropbox/CVR_Data_Shared/data_main"
  PATH_jbldir <- "~/Dropbox/CVR_Data_Shared/data_main/to-parquet/JBL/"
  PATH_parquet <- "~/Downloads/CVR_parquet"
}

match_field_name <- function(values, pattern) {
   str_detect(values,
	     sprintf("(^|\\|)\\s*%s\\s*(\\||$)",
	             pattern))
}

# See https://www.statology.org/r-add-column-if-does-not-exist/
add_cols <- function(df, cols) {
  add <- cols[!cols %in% names(df)]
  if(length(add) != 0) df[add] <- NA_character_
  return(df)
}

# Paths ----
filenames <- read_csv("R/build-harvard/input_files.txt", show_col_types = FALSE) |>
  pull(file)

infonames <- filenames |>
  str_replace("_long\\.", "_cvr_info.") |>
  str_replace("STATA_long", "STATA_cvr_info")

custom_cvr_info_field_mappings <- read_csv(
  path(PATH_jbldir, "csv/custom_cvr_info_field_mappings.csv"),
  show_col_types = FALSE)


out <- map_dfr(
  .x = infonames[1:10],
  .f = function(x, dir = PATH_projdir) {
    dat <- read_dta(fs::path(dir, "STATA_cvr_info", x), n_max = 10)

    # get state and county
    st <- parse_js_fname(x)["state"]
    ct <- parse_js_fname(x)["county"]

    mutate(across(everything(), as.character)) |>
      pivot_longer(
        everything(),
        names_to = "variable",
        values_to = "value") |>
      mutate(
        state = state,
        county = county,
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
