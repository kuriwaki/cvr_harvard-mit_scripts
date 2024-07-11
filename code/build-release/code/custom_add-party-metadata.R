
custom_add_party <- function(tbl) {
  tbl |>
    # fix Thomas Hall
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/174
    mutate(
      candidate = ifelse(
        state == "OHIO" & office == "STATE HOUSE" & district == "053" & candidate == "THOMAS HELL",
        "THOMAS HALL", candidate),
    ) |>
    # fix district in Mason, there is only one district and medsl has it wrong
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/277
    mutate(district = ifelse(state == "MICHIGAN" & county_name == "MASON" & district == "103", "101", district)) |>
    # Add missing party affiliations
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/pull/170
    left_join(read_delim("metadata/missing-party-metadata.txt", delim = ",", col_types = "ccccci"),
              by = c("state", "office", "candidate", "district"),
              relationship = "many-to-one") |>
    mutate(party_detailed = ifelse(is.na(party_detailed.x), party_detailed.y, party_detailed.x),
           party_detailed.x = NULL, party_detailed.y = NULL)
}
