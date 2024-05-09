std_county_name <- function(cn) {
  cn |>
    str_replace("^ED(?= \\d)", "DISTRICT") |>
    str_to_upper() |>
    str_replace_all("_", " ") |>
    str_replace("^SAINT", "ST.") |>
    str_replace("^ST\\s", "ST. ") |>
    # str_replace("JEFFERSON", "JEFF") |>
    str_squish() |>
    str_trim()
}
