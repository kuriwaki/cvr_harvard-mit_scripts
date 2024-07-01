#' Create the raw contests for manual classification
#'
#' @param paths A tibble of paths to the raw file, with 
#' the following columns: `path`, `state`, `county_name`, and `type`
#'
#' @return a tibble of raw contests
#' 
generate_contests <- function(paths) {
  process_files <- function(path, type, cnty, st) {
    message(sprintf("\nGenerating Raw Contests in %s, %s using file: %s", cnty, st, path))
    
    if (type == "delim") {
      if (is_header(path)) {
        contests = header_processor(path, n = 10) |> colnames()
      } else {
        contests = process_delim(path, n = 10) |> colnames()
      }
    } else if (type == "json") {
      contests = preprocess_json(path, contest_only = TRUE)
    } else if (type == "xml") {
      contests = preprocess_xml(path, contest_only = TRUE)
    } else if (type == "special") {
      contests = get_special_contests(st, cnty)
    } else {
      contests = "FAILED TO PARSE TYPE"
    }
    
    return(contests)
  }
  
  out <- paths |>
    filter(type != "xml") |>
    mutate(contest = future_pmap(
      list(path, type, county_name, state),
      possibly(process_files, quiet = FALSE)
    )) |>
    unnest(cols = contest) |>
    filter(!(contest %in% c(DROP_COLS, RENAME_COLS)))
  
  last_dplyr_warnings()
  problems(out)
  
  out2 <- paths |>
    filter(type == "xml") |>
    mutate(contest = pmap(
      list(path, type, county_name, state),
      possibly(process_files, quiet = FALSE)
    )) |>
    unnest(cols = contest) |>
    filter(!(contest %in% c(DROP_COLS, RENAME_COLS)))
  
  last_dplyr_warnings()
  problems(out2)
  
  bind_rows(out, out2) |> select(-path)
}

#' Create the base for the manual classification of contests
#' This function operates on some of the special counties with
#' unique layouts
#'
#' @inheritParams get_contests 
#'
#' @return A character vector of contests
generate_contests_special <- function(st, cnty) {
  if (st == "CALIFORNIA" & cnty == "LOS ANGELES") {
    read_csv("data/raw/California/Los Angeles/CandidateCodes.csv",
      show_col_types = FALSE,
      col_names = c("code", "candidate", "contest"), skip = 1, col_select = 3
    ) |>
      pull(contest)
  } else if (st == "FLORIDA") {
    files <- list.files(str_c("data/raw/Florida/", str_to_title(cnty)), pattern = "xls", full.names = TRUE)
    
    map(files, ~ read_excel(.x, n_max = 1, .name_repair = "unique_quiet")) |>
      list_rbind() |>
      colnames()
  } else if (st == "NEVADA" & cnty == "CLARK") {
    read_csv("data/raw/Nevada/Clark/cvr.csv",
      col_select = "Contest",
      show_col_types = FALSE
    ) |>
      distinct(Contest) |>
      pull(Contest)
  } else if (st == "NEW JERSEY" & cnty == "CUMBERLAND") {
    read_csv("data/raw/New Jersey/Cumberland/cvr.csv",
      show_col_types = FALSE,
      col_select = "Contest"
    ) |>
      distinct(Contest) |>
      pull(Contest)
  } else if (st == "TEXAS" & cnty == "DENTON") {
    read_csv("data/raw/Texas/Denton/cvr.csv",
      show_col_types = FALSE,
      col_select = "Race"
    ) |>
      distinct(Race) |>
      pull(Race)
  } else if (st == "TEXAS" & cnty == "MONTGOMERY") {
    read_csv("data/raw/Texas/Montgomery/cvr.csv",
      show_col_types = FALSE,
      col_select = 1,
      col_names = "contest"
    ) |>
      distinct(contest) |>
      pull(contest)
  } else {
    sprintf("UNKNOWN COMBO PASSED, GIVEN COUNTY: %s AND STATE: %s", cnty, st)
    return("FAILURE")
  }
}

#' Get the lookup table of contests for a specified state/county
#' combination. The table is generated manually by the research team
#'
#' @param st The full name of the state, in all caps
#' @param cnty The full name of the county, in all caps
#'
#' @return a `tibble` containing the lookup information
get_contests <- function(st, cnty) {
  read_csv(PATH_CONTESTS,
    col_types = cols(.default = col_character()),
    na = c("", "NA", "#N/A")
  ) |>
    mutate(county_name = replace_na(county_name, "")) |>
    filter(state == st, (county_name == cnty | state == "ALASKA")) |>
    drop_na(contest) |>
    mutate(across(-contest, str_to_upper)) |>
    mutate(
      district = ifelse(office %in% c("STATE HOUSE", "STATE SENATE", "US HOUSE"),
        str_pad(district, width = 3, side = "left", pad = "0"),
        district
      ),
      district = str_replace(district, fixed("COUNTY_NAME"), county_name),
      district = str_replace(district, fixed("STATEWIDE"), state),
      contest = iconv(contest, from = "ascii", to = "UTF-8", sub = "")
    )
}