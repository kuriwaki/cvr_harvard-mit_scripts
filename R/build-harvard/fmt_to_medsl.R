
#' Format to match MEDSL's variable names
#'
#'
fmt_harv_to_medsl <- function(tbl) {

  atlarge <- c("ALASKA", "DELAWARE", "WYOMING", "VERMONT", "MONTANA",
               "NORTH DAKOTA", "SOUTH DAKOTA")


  tbl |>
    mutate(
      county_name = str_to_upper(str_replace_all(county, "_", " ")),
      office = case_match(item,
                          "US_PRES" ~ "US PRESIDENT",
                          "US_SEN" ~ "US SENATE",
                          "US_SEN (S)" ~ "US SENATE",
                          "US_REP" ~ "US HOUSE",
                          "ST_SEN" ~ "STATE SENATE",
                          "ST_REP" ~ "STATE HOUSE",
                          "ST_GOV" ~ "GOVERNOR",
      ),
      candidate = str_to_upper(choice),
      state = case_match(state,
                         "AZ" ~ "ARIZONA",
                         "AK" ~ "ALASKA",
                         "CA" ~ "CALIFORNIA",
                         "CO" ~ "COLORADO",
                         "DE" ~ "DELAWARE",
                         "FL" ~ "FLORIDA",
                         "GA" ~ "GEORGIA",
                         "IL" ~ "ILLINOIS",
                         "LA" ~ "LOUISIANA",
                         "MD" ~ "MARYLAND",
                         "MI" ~ "MICHIGAN",
                         "NJ" ~ "NEW JERSEY",
                         "NM" ~ "NEW MEXICO",
                         "NV" ~ "NEVADA",
                         "OH" ~ "OHIO",
                         "OR" ~ "OREGON",
                         "RI" ~ "RHODE ISLAND",
                         "TN" ~ "TENNESSEE",
                         "TX" ~ "TEXAS",
                         "WV" ~ "WEST VIRGINIA",
                         "WI" ~ "WISCONSIN",
                         "UT" ~ "UTAH"
      ),
      party_detailed = case_match(party,
                                  "DEM" ~ "DEMOCRAT",
                                  "DEM?" ~ "DEMOCRAT",
                                  "REP" ~ "REPUBLICAN",
                                  "REP?" ~ "REPUBLICAN",
                                  "LBT" ~ "LIBERTARIAN",
                                  "IND" ~ "INDEPENDENT",
                                  "GRN" ~ "GREEN",
                                  "GRE" ~ "GREEN",
                                  "NPA" ~ "NO PARTY AFFILIATION",
                                  .default = party
      ),
      party_detailed = replace(party_detailed, choice %in% c("UNDERVOTE", "UNDERVOTE?"), "undervote"),
      party_detailed = replace(party_detailed, choice != "NA" & party_detailed == "LIB", "LIBERTARIAN"),
    ) |>
    # district formatting for MEDSL
    mutate(district = str_pad(dist, width = 3, pad = "0"),
           district = replace(district, office == "US PRESIDENT", "FEDERAL"),
           district = replace(district, office == "US HOUSE" & state %in% atlarge, "000")) |>
    mutate(dist_state = replace(state, !office %in% c("GOVERNOR", "US SENATE"), NA),
           district = coalesce(dist_state, district),
           district = replace(district, item == "US_SEN (S)" & state == "GEORGIA", "GEORGIA-III")) |> # class III senate seat
    select(state,
           county_name,
           matches("cvr_id"),
           matches("precinct"),
           matches("pres"),
           office,
           district,
           candidate,
           matches("column"),
           magnitude = num_votes,
           party,
           party_detailed)
}
