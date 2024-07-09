fmt_for_release <- function(tbl) {
  tbl |>
    select(-matches("jurisdiction_"), -matches("issue")) |>
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/308#issuecomment-2212053293
    filter(!(candidate %in% c("KANYE WEST", "JOE MCHUGH ELIZABETH STORM") & is.na(party_detailed) & state == "UTAH" & county_name == "SAN JUAN")) |>
    mutate(
      # harvard sometimes uses W-I for their writein candidate's party; change this to WRITEIN
      party_detailed = ifelse(candidate == "WRITE-IN" | party_detailed == "W-I", "WRITEIN", party_detailed),
      # standardize harvard candidate field to match medsl
      candidate = ifelse(candidate == "WRITE-IN", "WRITEIN", candidate),
      # standardized MEDSL's writein's parties to simply WRITEIN as well
      party_detailed = ifelse(candidate == "WRITEIN", "WRITEIN", party_detailed)
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
