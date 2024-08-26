fmt_for_release <- function(tbl) {
  tbl |>
    select(-matches("jurisdiction_"), -matches("issue"), -matches("^n$")) |>
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/308#issuecomment-2212053293
    filter(!(candidate %in% c("KANYE WEST", "JOE MCHUGH ELIZABETH STORM") & is.na(party_detailed) & state == "UTAH" & county_name == "SAN JUAN")) |>
    mutate(
      # harvard sometimes uses W-I for their writein candidate's party; change this to WRITEIN
      party_detailed = ifelse(candidate == "WRITE-IN" | party_detailed == "W-I", "WRITEIN", party_detailed),
      # standardize harvard candidate field to match medsl
      candidate = ifelse(candidate == "WRITE-IN", "WRITEIN", candidate),
      # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/194#issuecomment-2221716247
      candidate = ifelse(candidate == "NO SELECTION", "UNDERVOTE", candidate),
      # standardized MEDSL's writein's parties to simply WRITEIN as well
      party_detailed = ifelse(candidate == "WRITEIN", "WRITEIN", party_detailed)
      ) |>
    mutate(
      party = case_when(
        candidate %in% c("UNDERVOTE", "OVERVOTE", "NOT QUALIFIED") ~ NA_character_,
        party_detailed == "WRITEIN" ~ NA_character_,
        party_detailed == "DEMOCRAT" ~ "DEM",
        party_detailed == "REPUBLICAN" ~ "REP",
        party_detailed == "LIBERTARIAN" ~ "LBT",
        party_detailed == "GREEN" ~ "GRN",
        .default = "OTH"
      ),
      .before = party_detailed
    )
}
