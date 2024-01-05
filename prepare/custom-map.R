library(tidyverse)
library(urbnmapr)
library(sf)

county_sf <-  get_urbn_map("counties", sf = TRUE) |>
  rename(st = state_abbv) |>
  mutate(county_fips = as.numeric(county_fips))


statewide_sts <- c("DC", "AK", "RI", "DE")

county_stwide <- county_sf |>
  filter(st %in% statewide_sts) |>
  group_by(st, state_name, state_fips) |>
  summarize(geometry = sf::st_union(geometry)) |>
  mutate(county_fips = as.numeric(state_fips)*1000,
         county_name = "STATEWIDE")


map <- county_sf |>
  filter(!st %in% statewide_sts) |>
  bind_rows(county_stwide) |>
  mutate(county_disp = str_trim(str_remove(county_name, "(County|Parish)")))


write_rds(map, "data-val/county-statewide_map.rds")
