# Create validation dataset

library(dataverse)
library(tidyverse)
library(arrow)


# last line 9910028 - 63
tictoc::tic()
ret_all <- read_tsv(
  file = "~/Dropbox/CVR_Harvard-MIT/data/MEDSL/precincts20.sql", 
  skip = 62, 
  n_max = 9909966,
  col_names = c("precinct", "office", "party_detailed", "party_simplified", "mode", 
                "votes", "county_name", "county_fips", "jurisdiction_name", 
                "jurisdiction_fips", "candidate", "district",
                "dataverse", "year", "stage", "state", "special", "writein", 
                "state_po", "state_fips", "state_cen", "state_ic", "date", 
                "readme_check", "magnitude"), 
  col_types = "ccccciciciccciccllciiidli")
tictoc::toc()

# only some 
statewide <- c("ALASKA", "RHODE ISLAND")
ret_sel <- ret_all |> 
  tidylog::filter(office %in% c("US PRESIDENT", "US HOUSE", "US SENATE", 
                                "STATE HOUSE", "STATE SENATE", "GOVERNOR")) |> 
  mutate(jurisdiction_name = replace(jurisdiction_fips, state %in% statewide, NA)) |> 
  mutate(jurisdiction_fips = replace(jurisdiction_name, state %in% statewide, NA),
         county_name = replace(county_name, state %in% statewide, "STATEWIDE")) |> 
  arrange(state_fips, county_fips) |> 
  select(state, 
         matches("county"),
         matches("jurisdiction"),
         precinct, 
         mode, 
         office, 
         district,
         magnitude,
         special,
         writein,
         matches("party"), 
         candidate, 
         votes
         ) |> 
  # to match to Harvard on party
  mutate(writein = as.integer(writein),
         special = as.integer(special),
         party_detailed = case_when(candidate == "UNDERVOTES" ~ "UNDERVOTE", 
                                    candidate == "WRITEIN" ~ "WRITEIN", 
                                    candidate == "OVERVOTES" ~ "OVERVOTE", 
                                    .default = party_detailed)) |> 
  # match with Mason's definition of district
  mutate(district = replace(district, office == "US PRESIDENT", "FEDERAL")) |> 
  mutate(dist_state = replace(state, !office %in% c("GOVERNOR", "US SENATE"), NA),
         district = coalesce(dist_state, district),
         district = str_pad(district, width = 3, pad = "0")) # ALASKA ST SEN needs padding


county_mode_summ <- ret_sel |> 
  summarize(
    votes = sum(votes, na.rm = TRUE),
    .by = c("state", "county_name", "county_fips", "jurisdiction_name", 
            "jurisdiction_fips",  "office", "district", 
            "magnitude",  "special", "writein",
            "mode",
            "party_detailed", "party_simplified", "candidate", 
            )
  )
# DISTRICT 1, POSITION 2
county_summ <- county_mode_summ |> 
  summarize(
    votes = sum(votes, na.rm = TRUE),
    votes = sum(votes, na.rm = TRUE),
    .by = c("state", "county_name", "county_fips", "jurisdiction_name", 
            "jurisdiction_fips",  "office", "district", 
            "magnitude",  "special", "writein",
            "party_detailed", "party_simplified", "candidate", 
    )
  )

# write to parquet
write_pq <- function(obj, pq_name, dir = "~/Dropbox/CVR_parquet/returns/") {
  obj |> 
    group_by(state, office) |> 
    write_dataset(
      pq_name, 
      format = "parquet",
      existing_data_behavior = "delete_matching"
    )
}

write_pq(ret_sel, "returns/by-precinct_state")
write_pq(county_summ, "returns/by-county")
write_pq(county_mode_summ, "returns/by-county-mode")
