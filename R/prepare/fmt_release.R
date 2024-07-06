fmt_for_release <- function(tbl) {
  tbl |>
    select(-matches("jurisdiction_")) |>
    mutate(
      # harvard sometimes uses W-I for their writein candidate's party; change this to WRITEIN
      party_detailed = replace(party_detailed, candidate == "WRITE-IN" | party_detailed == "W-I", "WRITEIN"),
      # standardize harvard candidate field to match medsl
      candidate = replace(candidate, candidate == "WRITE-IN", "WRITEIN"),
      # standardized MEDSL's writein's parties to simply WRITEIN as well
      party_detailed = replace(party_detailed, candidate == "WRITEIN", "WRITEIN")
      ) |>
    mutate(
      party = NA_character_,
      party = ifelse(party_detailed == "DEMOCRAT",    "DEM", party),
      party = ifelse(party_detailed == "REPUBLICAN",  "REP", party),
      party = ifelse(party_detailed == "LIBERTARIAN", "LBT", party),
      party = ifelse(party_detailed == "GREEN",       "GRN", party),
      party = ifelse(party_detailed == "WRITEIN",     "W-I", party),
      party = ifelse(!party %in% c("DEM", "REP", "LBT", "GRN", "W-I"), "OTH", party),
      .before = party_detailed
    )
}
