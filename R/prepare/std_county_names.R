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


fix_county_name <- function(cn) {
  cn |>
    case_match(
      "SAN BERARDINO" ~ "SAN BERNARDINO",
      "PRINCE GEORGE" ~ "PRINCE GEORGE'S",
      "TUSCAWARAS" ~ "TUSCARAWAS",
      "CHAVEZ" ~ "CHAVES",
      "EMMIT" ~ "EMMET",
      "PALM" ~ "PALM BEACH",
      "JOE DAVIESS" ~ "JO DAVIESS",
      "BALTIMORE COUNTY" ~ "BALTIMORE",
      "BLECKLY" ~ "BLECKLEY",
      "CHATOOGA" ~ "CHATTOOGA",
      "EMMANUEL" ~ "EMANUEL",
      "BLOOMINGTON" ~ "MCLEAN",
      "GUADELUPE" ~ "GUADALUPE",
      "GLASKCOCK" ~ "GLASCOCK",
      "MIAMI DADE" ~ "MIAMI-DADE",
      "OUTGAMIE" ~ "OUTAGAMIE",
      "KING" ~ "KINGS",
      "QUEEN ANNE" ~ "QUEEN ANNE'S",
      "CHIPPEWAH" ~ "CHIPPEWA",
      "EMMETT" ~ "EMMET",
      .default = cn
    )
}
