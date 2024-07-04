fmt_for_release <- function(tbl) {
  tbl |>
    select(-matches("jurisdiction_")) |>
    mutate(
      party = NA_character_,
      party = ifelse(party_detailed == "DEMOCRAT",    "DEM", party),
      party = ifelse(party_detailed == "REPUBLICAN",  "REP", party),
      party = ifelse(party_detailed == "LIBERTARIAN", "LBT", party),
      party = ifelse(party_detailed == "GREEN",       "GRN", party),
      party = ifelse(party_detailed == "WRITE-IN",    "W-I", party),
      party = ifelse(!party %in% c("DEM", "REP", "LBT", "GRN", "W-I"), "OTH", party),
      .before = party_detailed
    )
}
