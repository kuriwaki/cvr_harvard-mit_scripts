library(tidyverse)
library(tidycensus)
# only tweaking Jeff Lewis code, but outside of targets

#' Helper count for New England townships
town2cty <- function() {
  tidycensus::census_api_key("a71658d1b3df0f36e32876b12a119dc398dd84b3")

  n2a <- state.abb
  names(n2a) <- state.name
  state <- get_decennial(geography = "state",
                         variables = "P1_001N",
                         year = 2020,
                         state = c("CT", "RI", "ME", "VT", "NH", "MA"),
                         sumfile = "pl") |>
    glimpse() |>
    transmute(stateFipsCode = GEOID,
              stateName = NAME,
              state_abb = n2a[NAME])

  cty <- get_decennial(geography = "county",
                       variables = "P1_001N",
                       year = 2020,
                       state = c("CT", "RI", "ME", "VT", "NH", "MA"),
                       sumfile = "pl")

  town <- get_decennial(geography = "county subdivision",
                        variables = "P1_001N",
                        year = 2020,
                        state = c("CT", "RI", "ME", "VT", "NH", "MA"),
                        sumfile = "pl")
  town |>
    transmute(countyFipsCode  = str_sub(GEOID, 1, 5),
              stateFipsCode = str_sub(GEOID, 1, 2),
              town = map_chr(str_split(NAME, ","),
                             function(z) z[[1]]) |>
                str_to_upper(),
              county = str_extract(NAME, "(?<=, ).+?(?= County)") |>
                str_to_upper()
    ) |>
    mutate( town = str_replace(town, " TOWN$", "")) |>
    left_join(state, by = "stateFipsCode")
}


a2n <- c(state.abb, "DC")
names(a2n) <- c(state.name, "District of Columbia")


dat <- read_csv("data-val/pres2020gen_county_CNN.csv.gz",
                show_col_types = FALSE) |>
  mutate(state_abb = a2n[stateName],
         countyName = countyName |>
           str_to_upper() |>
           str_replace_all("\\.", "")) |>
  pivot_longer(cols = c(Trump, Biden),
               names_to = "choice",
               values_to = "votes_reported")

town2cty <- town2cty()

dat_clean <- dat |>
  filter(state_abb %in% c("CT", "MA", "ME", "NH", "RI", "VT", "DC")) |>
  left_join(town2cty, by = c("state_abb", "countyName" = "town")) |>
  mutate(stateName = stateName.x,
         countyName = county,
         countyFipsCode = countyFipsCode.y,
         countyName = if_else(state_abb == "DC",
                              "DISTRICT OF COLUMBIA",
                              state_abb)) |>
  group_by(stateName, countyName, countyFipsCode, state_abb, choice) |>
  summarize(votes_reported = sum(votes_reported)) |>
  bind_rows(
    dat |>
      filter(!(state_abb %in% c("CT", "MA", "ME", "NH", "RI", "VT", "DC")))
  ) |>
  ungroup() |>
  arrange(stateName, countyName) |>
  relocate(st = state_abb,
         state = stateName,
         county_name = countyName,
         county_fips = countyFipsCode) |>
  mutate(county_fips = as.numeric(county_fips))

write_csv(dat_clean, "data-val/cnn_county.csv")
