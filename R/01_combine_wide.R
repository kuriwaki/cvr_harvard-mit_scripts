# create dataset like

# st office dist  candidate party votes_h votes_m
# AL ST_HOU  001 JOHN SMITH     R    1299    1300

library(duckplyr)
library(arrow)
library(tidyverse)


dsa_h <- open_dataset("harvard")
dsa_m <- open_dataset("medsl")
dsa_v <- open_dataset("returns/by-county/")

parties_use <- c("DEMOCRAT", "REPUBLICAN", "LIBERTARIAN", "GREEN", 
                 "UNDERVOTE", "UNDERVOTES", "undervote",
                 "WRITEIN", "WRITE-IN")

# counts ---

## Harvard
count_h <- dsa_h |> 
  # TODO: do this beforehand
  filter(!(state == "ARIZONA" & office == "STATE HOUSE"),
         party_detailed %in% parties_use) |> 
  count(state, county_name, office, district, candidate, party_detailed,
        name = "votes") |> 
  collect() |> 
  mutate(party_detailed = recode(party_detailed, "undervote" = "UNDERVOTE")) |> 
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |> 
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |> 
  rename(candidate_h = candidate, votes_h = votes)

## MIT
count_m <- dsa_m |> 
  filter(!(state == "ARIZONA" & office == "STATE HOUSE"),
         party_detailed %in% parties_use) |> 
  count(state, county_name, office, district, 
        candidate, party_detailed, contest,
        name = "votes") |> 
  collect() |> 
  # match with Mason's definition of district -- remove when issues like https://github.com/kuriwaki/CVR_Harvard-MIT/issues/12
  mutate(district = replace(district, office == "US PRESIDENT", "FEDERAL")) |> 
  mutate(dist_state = replace(state, !office %in% c("GOVERNOR", "US SENATE"), NA),
         district = coalesce(dist_state, district),
         district = replace(district, state %in% c("ALASKA", "DELAWARE") & office == "US HOUSE", "000"),
         dist_state = NULL) |> 
  mutate(
    county_name = replace(county_name, state %in% c("ALASKA", "RHODE ISLAND"), "STATEWIDE"),
    district = replace(district, state == "GEORGIA" & str_detect(contest, "Special"), "GEORGIA-III"),
    district = str_pad(district, width = 3, pad = "0")) |>  # TEMP FIX RHODE ISLAND
  # count across "context" after modifying GA
  count(state, county_name, office, district, 
        candidate, party_detailed, 
        name = "votes", wt = votes) |> 
  # https://github.com/kuriwaki/CVR_Harvard-MIT/issues/15
  mutate(party_detailed = case_when(candidate == "UNDERVOTE" ~ "UNDERVOTE", 
                                    candidate == "WRITEIN" ~ "WRITEIN", 
                                    candidate == "OVERVOTE" ~ "OVERVOTE", 
                                    .default = party_detailed)) |> 
  # top-two
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |> 
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |> 
  rename(candidate_m = candidate, votes_m = votes) |> 
  mutate(county_name = str_to_upper(county_name))

## Validation
count_v <- dsa_v |> 
  filter(!(state == "ARIZONA" & office == "STATE HOUSE"),
         party_detailed %in% parties_use | is.na(party_detailed)) |> 
  count(state, county_name, office, district, candidate, party_detailed, writein, 
        special,
        wt = votes,
        name = "votes") |> 
  collect() |> 
  mutate(district = replace(district, state == "GEORGIA" & special, "GEORGIA-III")) |> 
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |> 
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |> 
  rename(candidate_v = candidate, votes_v = votes)

# Together ------
out <- count_h |> 
  full_join(count_m, by = c("state", "county_name", "office", "district", "party_detailed", "cand_rank")) |> 
  left_join(count_v, by = c("state", "county_name", "office", "district", "party_detailed", "cand_rank")) |> 
  select(-cand_rank) |> 
  arrange(state, county_name, desc(office), district, party_detailed) |> 
  relocate(state:district, party_detailed, special, writein) 

out |> 
  writexl::write_xlsx("combined/by-county-candidate_compare.xlsx")



# counties for harvard
out |> 
  filter(is.na(candidate_h), !is.na(candidate_m), !is.na(candidate_v),
         party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |> 
  summarize(.by = c(state, county_name), 
            n_pres = sum(votes_m * (office == "US PRESIDENT"), na.rm = TRUE)
            ) |> 
  write_csv("~/Downloads/counties-not-in-harvard.csv")




out |> 
  filter(office == "US PRESIDENT", party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |> 
  summarize(sum_h = sum(votes_h, na.rm = TRUE),
            sum_m = sum(votes_m, na.rm = TRUE),
            sum_v = sum(votes_v, na.rm = TRUE),
            .by = c(state, county_name)) |> 
  writexl::write_xlsx("combined/by-county_compare.xlsx")  
