################################################
################################################
################################################
### NOTE: This file is not run as part of
###       the main pipeline, and was used
###       interactively to generate the
###       crosswalk.
################################################
################################################
################################################

library(arrow)
library(tidyverse)

cvr_parquet_PATH = "~/Dropbox/CVR_parquet/intermediate/coalesced"
medsl_precinct_parquet_PATH = "~/Dropbox/MEDSL_Precinct_2020/parquet"
state2abb <- state.abb
names(state2abb) <- stringr::str_to_upper(state.name)

#
# Helper functions
#

# Return dataframe of all counties in the CVR collection
county_list <- function() {
  arrow::open_dataset(cvr_parquet_PATH) |>
    distinct(state, county_name) |>
    collect()
}

#
# Transform precinct labels in CVR collection to more closely match
# MEDSL precinct labels
#
patch_cvr_precinct <- function(the_state, the_county, precinct.x) {
   case_when(
               ## Arizona
               the_county == "MARICOPA" & the_state == "ARIZONA" ~
                 str_extract(precinct.x, "^\\d{4}"),
               ## California
               the_county %in%  c("PLACER", "RIVERSIDE", "FRESNO") &
                  the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^\\d+"),
               the_county == "SAN DIEGO" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "(?<=\\-)\\d{6}(?=\\-)"),
               the_county %in% c("KINGS", "ALAMEDA") &
                   the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^\\d+(?= \\()"),
               the_county == "TEHAMA" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "(?<=00)\\d+"),
               the_county == "KERN" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "\\d{7}"),
               the_county == "CONTRA COSTA" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^[A-Z0-9]+(?= \\d)"),
               the_county %in% c("MENDOCINO", "MERCED") &
                   the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^\\d+"),
               the_county == "SONOMA" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^[0-9]+(?= )"),
               the_county == "TUOLUMNE" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^\\d{6}"),
               the_county %in% "YUBA" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^\\d{4}"),
               the_county %in% "SAN BERNARDINO" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, ".{7}$"),
               the_county %in% "SAN MATEO" & the_state == "CALIFORNIA" ~
                 str_extract(precinct.x, "^\\d{4}"),
               ## Colorado
               the_county %in% c("ROUTT", "ARAPAHOE", "MOFFAT") &
                    the_state == "COLORADO" ~
                 str_extract(precinct.x, "(?<=\\().+?(?=( |\\)))"),
               the_county == "DENVER" & the_state == "COLORADO" ~
                 str_extract(precinct.x, "^\\d+(?= \\()"),
               the_county %in% c("MESA", "LARIMER", "EAGLE", "EL PASO",
                                 "ADAMS", "PUEBLO", "YUMA", "MOFFAT",
                                 "JEFFERSON", "TELLER") &
                    the_state == "COLORADO" ~
                 str_extract(precinct.x, "^\\d+(?= \\-)"),
               the_county == "WELD" & the_state == "COLORADO" ~
                 str_extract(precinct.x, "^\\d+(?= \\-)"),
               the_state == "COLORADO" ~
                 str_extract(precinct.x, "^\\d{10}"),
               ## Georgia
               the_county == "COLUMBIA" & the_state == "GEORGIA" ~
                 str_replace(precinct.x, "^\\d{3}\\-", "") |>
                 str_replace("[0-9][A-Z]?$", "") |>
                 str_replace("CC$", "") |>
                 str_replace("CO$", "") |>
                 str_replace("2C$", "") |>
                 str_replace("(?<=LPG)[OC]+", "") |>
                 str_replace("(?<=HBL).$", "") |>
                 str_replace("(?<=GPSS)..$", ""),
               the_county == "BIBB" & the_state == "GEORGIA" ~
                 str_replace(precinct.x, "^\\d{3}\\-", "") |>
                 str_replace("[A-Z]$", ""),
               the_county == "EARLY" & the_state == "GEORGIA" ~
                 str_extract(precinct.x, "(?<=\\-)[A-Z][SU]?") |>
                 str_replace("D", "A") |>
                 str_replace("J", "CS"),
               the_county == "GWINNETT" & the_state == "GEORGIA" ~
                 str_extract(precinct.x, "(?<=\\-)\\d{3}"),
               the_county == "COBB" & the_state == "GEORGIA" ~
                 str_extract(precinct.x, ".{4}$"),
               the_state == "GEORGIA" ~
                 str_extract(precinct.x, "(?<=\\-).+?$"),
               the_county == "DICKINSON" & the_state == "IOWA" ~
                 str_extract(precinct.x, "(?<=No\\. )(\\d+)"),
               the_county %in% c("HARFORD", "PRINCE GEORGE'S") &
                   the_state == "MARYLAND" ~
                 str_replace(precinct.x, "^0", ""),
               the_county == "CHARLEVOIX" & the_state == "MICHIGAN" ~
                 str_extract(precinct.x, "(?<=Precinct )\\d+"),
               ## Michigan
               the_county == "CLINTON" & the_state == "MICHIGAN" ~
                 str_extract(precinct.x, "\\d+"),
               ## Nevada
               the_county == "DOUGLAS" & the_state == "NEVADA" ~
                 str_extract(precinct.x, "^\\d+"),
               the_county == "PERSHING" & the_state == "NEVADA" ~
                 str_extract(precinct.x, "PRECINCT \\d+"),
               the_county == "LYON" & the_state == "NEVADA" ~
                 str_extract(precinct.x, ".+?\\d+"),
               ## New Jersey
               the_county == "BERGEN" & the_state == "NEW JERSEY" ~
                 str_replace(precinct.x, " \\- Ward \\d", "") |>
                 str_replace("(Borough|City|Township|Village) of", ""),
               the_county %in% c("PASSAIC", "MONMOUTH") &
                   the_state == "NEW JERSEY" ~
                 str_extract(precinct.x, ".+?(?= (\\- \\d)|Cong|Mail|Prov)") |>
                 str_trim() |>
                 str_replace("boro boro", "boro"),
               the_county == "SALEM" & the_state == "NEW JERSEY" ~
                 str_extract(precinct.x, ".+?(?= District|East|West)"),
               the_county == "GLOUCESTER" & the_state == "NEW JERSEY" ~
                 str_replace(precinct.x, "(\\- WARD|DISTRICT).+", "") |>
                 str_replace("(BOROUGH|CITY|TOWNSHIP) OF ", "") |>
                 str_trim(),
               ## Ohio
               the_county == "BELMONT" & the_state == "OHIO" ~
                 str_replace(precinct.x, "^[0-9.,]+", ""),
               the_county == "WAYNE" & the_state == "OHIO" ~
                 str_extract(precinct.x, "^[0-9]+"),
               the_county %in% c("ASHTABULA", "WOOD") & the_state == "OHIO" ~
                 str_replace(precinct.x, "^[0-9.]+? ", ""),
               the_county %in% c("HARRISON", "GREENE") & the_state == "OHIO" ~
                 str_extract(precinct.x, "^\\d+"),
               the_county == "RICHLAND" & the_state == "OHIO" ~
                 str_extract(precinct.x, "^.+?(?=\\-)"),
               ## Oregon
               the_county == "MARION" & the_state == "OREGON" ~
                 str_extract(precinct.x, "^\\d+"),
               the_county == "YAMHILL" & the_state == "OREGON" ~
                 str_extract(precinct.x, "^\\d+") |>
                 str_replace("^0*", ""),
               ## Texas
               the_county == "FORT BEND" & the_state == "TEXAS" ~
                 str_extract(precinct.x, "^\\d+"),
               the_county %in% c("TAYLOR", "HIDALGO", "COLLIN") &
                  the_state == "TEXAS" ~
                 str_extract(precinct.x, "^\\d{3}A?"),
               the_county == "SMITH" &
                  the_state == "TEXAS" ~
                 str_extract(precinct.x, "^\\d+") |>
                 str_replace("^0+", "") |>
                 as.integer() %>%
                 sprintf("%03i", .),
               the_county == "POTTER" &
                 the_state == "TEXAS" ~
                 str_replace(precinct.x, " County Only\\s*", ""),
               TRUE ~ precinct.x
            )
}

# Transform certain MEDSL precinct labels to better match them to
# CVR precinct labels
patch_mdsl_precinct <- function(the_state,
                                 the_county,
                                 precinct.y) {
  case_when(
              the_county == "MARICOPA" & the_state == "ARIZONA" ~
                str_extract(precinct.y, "^\\d{4}"),
              the_state == "CALIFORNIA" ~
                str_extract(precinct.y, "(?<=\\d{5}).+?(?=\\_)"),
              the_county == "DOUGLAS" & the_state == "NEVADA" ~
                str_extract(precinct.y, "^\\d+"),
              the_county == "GREENE" & the_state == "OHIO" ~
                str_extract(precinct.y, "\\d+$"),
              the_county == "ASHTABULA" ~
                str_replace(precinct.y, "^PRECINCT ", ""),
              the_county %in% c("TAYLOR", "HIDALGO", "COLLIN",
                                "SMITH", "POTTER") &
                  the_state == "TEXAS" ~
                str_extract(precinct.y, "\\d{3}$"),
              the_county %in% c("FORT BEND") &
                the_state == "TEXAS" ~
                str_extract(precinct.y, "\\d{4}$"),
              TRUE ~ precinct.y
  )
}

#
# Main function to map CVR precincts in a given county to
# MEDSL precincts
#
match_county <- function(the_state = "WISCONSIN",
                         the_county = "BROWN") {
  cat(sprintf("Working on %s, %s.\n", the_county, the_state))
  cvr <- arrow::open_dataset(cvr_parquet_PATH) |>
            filter(county_name == the_county,
                   state == the_state,
                   !is.na(party_detailed))
  glimpse(cvr)

  number_of_precincts <- cvr |>
                          distinct(precinct) |>
                          collect() |>
                          pull() |>
                          length()

  cat(sprintf("\tFound %i precincts in the CVR data.\n", number_of_precincts))

  prec_mdsl <- open_dataset(medsl_precinct_parquet_PATH,
                            unify_schemas = FALSE,
                            schema = schema(field("county_name", string()),
                                            field("state_po", string()),
                                            field("precinct", string()),
                                            field("office", string()),
                                            field("district", string()),
                                            field("party_simplified", string()),
                                            field("mode", string()),
                                            field("votes", double()))) |>
                mutate(county_name = if_else(the_state == "RHODE ISLAND",
                                             "STATEWIDE", county_name)) |>
                filter(state_po == state2abb[the_state],
                       county_name == the_county,
                       !is.na(party_simplified)) |>
                collect() |>
                mutate(m.precinct = patch_mdsl_precinct(the_state,
                                                        the_county,
                                                        precinct)) |>
                mutate(nnn = n(),  .by = c("m.precinct", "office",
                                           "district", "party_simplified")) |>
                filter(case_when(n() > 1 & mode != "TOTAL" ~ TRUE,
                                n() == 1 ~ TRUE,
                                TRUE ~ FALSE),
                                .by = c("m.precinct", "office",
                                        "district", "party_simplified")) |>
                summarize(votes = sum(votes),
                          precinct = paste0(unique(precinct), collapse = "|"),
                          .by = c("m.precinct", "office",
                                  "district", "party_simplified")) |>
               filter(party_simplified %in% c("DEMOCRAT", "REPUBLICAN"))

  number_of_mdsl_precincts <- prec_mdsl |>
                                distinct(precinct) |>
                                pull() |>
                                length()
  cat(sprintf("\tFound %i precincts in the MDSL precinct data.\n",
              number_of_mdsl_precincts))

  cvr |>
    count(precinct, office, district, party_detailed) |>
    collect() |>
    mutate(m.precinct = patch_cvr_precinct(the_state,
                                            the_county, precinct)) |>
    filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN")) |>
    summarize(n = sum(n),
              precinct = paste0(sort(unique(precinct)), collapse = "|"),
              .by =c("m.precinct", "office", "district", "party_detailed")) |>
    mutate(district = case_when(district == "FEDERAL" ~ "STATEWIDE",
                                TRUE ~ district)) |>
    left_join(prec_mdsl,
              by = c("office",
                "district",
                "party_detailed" = "party_simplified"),
              relationship = "many-to-many") |>
    summarize( discrepancy = sum(abs(n - votes), na.rm = TRUE),
               cvr_votes = sum(n, na.rm = TRUE),
               prec_votes = sum(votes, na.rm = TRUE),
               offices = paste0(sort(unique(office)), collapse = "|"),
               precinct.x = paste0(unique(precinct.x), collapse = "|"),
               precinct.y = paste0(unique(precinct.y), collapse = "|"),
             .by = c('m.precinct.x', 'm.precinct.y')) |>
    mutate(name_ed_dist = stringdist::stringdist(str_to_upper(m.precinct.x),
                                                 str_to_upper(m.precinct.y))) |>
    arrange('m.precinct.x', discrepancy) |>
    filter(case_when(name_ed_dist == 0 ~ TRUE,
                     (min(name_ed_dist, na.rm = TRUE) > 0) &
                       (discrepancy + name_ed_dist ==
                        min(discrepancy + name_ed_dist, na.rm = TRUE)) ~ TRUE,
                     TRUE ~ FALSE),
          .by = "m.precinct.x") |>
    mutate(across(c("precinct.y",
                    "discrepancy",
                    "prec_votes",
                    "name_ed_dist"),
                  function(v) if_else(discrepancy != min(discrepancy) &
                                      name_ed_dist != 0, NA, v)),
          .by = 'm.precinct.y') |>
    mutate(m.precinct.y = if_else(is.na(precinct.y), NA, m.precinct.y)) |>
    rename(cvr_precinct = precinct.x, mdsl_precinct = precinct.y) |>
    mutate(times = sum(!is.na(mdsl_precinct)),
           .by = "cvr_precinct") |>
    mutate(state = the_state,
           county_name = the_county,
           .before = 1) |>
    select(state, county_name, cvr_precinct, mdsl_precinct, everything())
}

#
# Example...
#
#county_list()
#z <- match_county(the_county = "HUMBOLDT", the_state = "CALIFORNIA")
#glimpse(z)
