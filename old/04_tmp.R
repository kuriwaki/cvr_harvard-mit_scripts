# office x county x dist x cand x party x N

library(tidyverse)
library(arrow)
library(haven)

countycodes <- read_csv("data/countycodes.csv")

# MEDSL

medsl_parq <- open_dataset("data/MEDSL/cvrs_statewide/")

medsl_long <- medsl_parq |>
  count(office, candidate,
        district, county_name,
        state, party_detailed) |> collect()

medsl_long <- medsl_long |>
  mutate(county_name = std_county_name(fix_county_name(county_name)),
         county_name = ifelse(state == "MARYLAND" & county_name == "ST. MARY", "ST. MARY'S", county_name),
         county_name = ifelse(state == "MICHIGAN" & county_name == "DICKENSON", "DICKINSON", county_name),
         county_name = ifelse(state == "ALASKA", "STATEWIDE", county_name)) |>
  left_join(countycodes |> select(state, county_name, county_fips) |> mutate(state = str_to_upper(state)))




