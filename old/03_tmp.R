library(tidyverse)
library(arrow)
library(haven)
library(urbnmapr)
source("R/std_county_names.R")

# Use data/countycodes.csv as county keys
countycodes <- read_csv("data/countycodes.csv")

# Get county-level count of Trump and Biden votes from MEDSL
medsl_parq <- open_dataset("data/MEDSL/cvrs_statewide/")

medsl_pres <- medsl_parq |>
  to_duckdb() |>
  filter(office == "US PRESIDENT")

## Votes are counted either as NA or 1
medsl_count <- medsl_pres |>
  group_by(state, county_name) |>
  filter(str_detect(candidate, "BIDEN|TRUMP")) |>
  count(candidate, voted) |> collect() |>
  filter(is.na(voted) | voted == 1) |>
  ungroup()

medsl_count <- medsl_count |>
  mutate(county_name = std_county_name(fix_county_name(county_name)),
         county_name = ifelse(state == "ALASKA", "STATEWIDE", county_name),
         county_name = ifelse(state == "MARYLAND" & county_name == "ST. MARY", "ST. MARY'S", county_name)) |>
  left_join(countycodes |> mutate(state = str_to_upper(state)),
            by = c("county_name" = "county_name", "state" = "state"))

medsl_count <- medsl_count |>
  mutate(party = ifelse(str_detect(candidate, "BIDEN"), "DEM", "REP")) |>
  select(-c(candidate)) |>
  pivot_wider(id_cols = c(state, county_name, county_fips), names_from = party, values_from = n) |>
  mutate(sum_DR = DEM + REP)

# Get county-level votes for Trump and Biden from Harvard

filepaths <- list.files("../CVR_Data_Shared/data_main/STATA_long/", full.names = TRUE) |>
  str_subset("2022", negate = TRUE) |>
  str_subset("_prim_", negate = TRUE) |>
  str_subset("CA_Los_Angeles_long.dta", negate = TRUE)

names(filepaths) <- fs::path_file(filepaths) |> str_remove("(_long.dta|.long_\\d.dta)")

harv_counts <- map(
  filepaths,
  \(x) {
    read_dta(x) |>
      rename(any_of(c(candidate = "choice", office = "item"))) |>
      filter(office %in% c("US_PRES")) |>
      count(office, column, choice_id, candidate)
  },
  .progress = TRUE) |>
  list_rbind(names_to = "filename") |>
  separate_wider_delim(
    filename,
    delim = "_",
    too_many = "merge",
    names = c("state", "county")) |>
  mutate(county = str_remove(county, "_long.dta"))

## Separately join Orange and San Diego (troubleshooting)
ca_addtl <- filepaths[filepaths |> str_detect("CA_Orange|CA_San_D")]

ca_addtl_count <- map(
  ca_addtl,
  \(x) {
    read_dta(x) |>
      rename(any_of(c(candidate = "choice", office = "item"))) |>
      filter(office %in% c("US_PRES", "PRESIDENT AND VICE PRESIDENT")) |>
      count(office, column, choice_id, candidate) # merge in cvr_info?
  },
  .progress = TRUE) |>
  list_rbind(names_to = "filename") |>
  separate_wider_delim(
    filename,
    delim = "_",
    too_many = "merge",
    names = c("state", "county")) |>
  mutate(county = str_remove(county, "_long.dta"))

## Append CA to main Harvard count
harv_counts <- rbind(harv_counts, ca_addtl_count)

## Merge in choice-level data — only party for now — and county FIPS
harv_counts <- harv_counts |>
  left_join(read_dta("../CVR_Data_Shared/data_main/item_choice_info.dta") |>
              select(state, county, column, choice_id, party))

harv_counts <- harv_counts |> mutate(county = fix_county_name(std_county_name(county))) |>
  rename(st = "state") |>
  left_join(countycodes, by = c("st" = "st", "county" = "county_name")) |> filter(party %in% c("DEM", "REP"))

harv_counts <- harv_counts |> group_by(state, county, office, choice_id, candidate, party) |>
  mutate(n = sum(n)) |> ungroup() |>
  select(-c(office, column, choice_id, candidate)) |>
  distinct() |>
  pivot_wider(id_cols = c(state, county, county_fips), names_from = party, values_from = n) |>
  mutate(sum_DR = DEM + REP)

# Use validation-pres.csv as ground truth

validation <- read.csv("data/validation-pres.csv") |>
  mutate(party = ifelse(choice == "Biden", "DEM", "REP"))

validation <- validation |> group_by(st, state, county_name, county_fips, choice, party) |>
  mutate(votes_reported = sum(votes_reported)) |> ungroup() |>
  select(-c(st_fips, wt, n, choice)) |> distinct() |>
  pivot_wider(id_cols = c(st, state, county_name, county_fips),
              names_from = party, values_from = votes_reported, names_prefix = "val_") |>
  mutate(val_DR = val_DEM + val_REP)

## Merge validation to MEDSL, compute coverage as prop of total D/R votes

medsl_count <- medsl_count |> filter(!is.na(county_fips)) |>
  left_join(validation |> select(-c(state, county_name)),
                         by = c("county_fips" = "county_fips")) |>
  mutate(cov_DR = sum_DR/val_DR)

## Merge validation to Harvard, compute coverage as prop of total D/R votes

harv_counts <- harv_counts |> left_join(validation,
                         by = c("county_fips" = "county_fips",
                                "state" = "state",
                                "county" = "county_name")) |>
  mutate(cov_DR = sum_DR/val_DR)

# Create map

county_map <- read_rds("data/val/county-statewide_map.rds")

state_map <- get_urbn_map("states", sf = TRUE) |> rename(st = state_abbv)

## Harvard coverage
ggplot(county_map |> left_join(harv_counts)) + geom_sf(data = state_map, linewidth = .5, inherit.aes = FALSE, fill = "transparent") +
  geom_sf(aes(fill = cov_DR), linewidth = .1) +
  ggthemes::theme_map() + scale_fill_continuous(na.value = "transparent") + ggtitle("Harvard coverage")

## MEDSL coverage
ggplot(county_map |> left_join(medsl_count |> select(-c(county_name)))) + geom_sf(data = state_map, linewidth = .5, inherit.aes = FALSE, fill = "transparent") +
  geom_sf(aes(fill = cov_DR), linewidth = .1) +
  ggthemes::theme_map() + scale_fill_continuous(na.value = "transparent") + ggtitle("MEDSL coverage")

## Coverage > .8
ggplot(county_map |>
         left_join(harv_counts |>
                     transmute(cov_harv = cov_DR > .8,
                               problem_harv = cov_DR > 1.05,
                               county_fips)) |>
                     left_join(medsl_count |>
                                 transmute(cov_mit = cov_DR > .8,
                                           problem_mit = cov_DR > 1.05,
                                           county_fips)) |>
                     mutate(cov_harv = ifelse(is.na(cov_harv), FALSE, cov_harv),
                            problem_harv = ifelse(is.na(problem_harv), FALSE, problem_harv),
                            cov_mit = ifelse(is.na(cov_mit), FALSE, cov_mit),
                            problem_mit = ifelse(is.na(problem_mit), FALSE, problem_mit),
                            both = ifelse(cov_harv & cov_mit, "Both",
                                          ifelse(cov_harv & !cov_mit, "Harvard only",
                                                 ifelse(!cov_harv & cov_mit, "MEDSL only", NA))))) +
  geom_sf(data = state_map, linewidth = .5, inherit.aes = FALSE, fill = "transparent") +
  geom_sf(aes(fill = both), linewidth = .1) +
  ggthemes::theme_map() + scale_fill_discrete(na.value = "transparent") + ggtitle("Coverage > .8")

## Coverage > 1.05
ggplot(county_map |>
         left_join(harv_counts |>
                     transmute(cov_harv = cov_DR > .8,
                               problem_harv = cov_DR > 1.05,
                               county_fips)) |>
         left_join(medsl_count |>
                     transmute(cov_mit = cov_DR > .8,
                               problem_mit = cov_DR > 1.05,
                               county_fips)) |>
         mutate(cov_harv = ifelse(is.na(cov_harv), FALSE, cov_harv),
                problem_harv = ifelse(is.na(problem_harv), FALSE, problem_harv),
                cov_mit = ifelse(is.na(cov_mit), FALSE, cov_mit),
                problem_mit = ifelse(is.na(problem_mit), FALSE, problem_mit),
                both = ifelse(problem_harv & problem_mit, "Both",
                              ifelse(problem_harv & !problem_mit, "Harvard only",
                                     ifelse(!problem_harv & problem_mit, "MEDSL only", NA))))) +
  geom_sf(data = state_map, linewidth = .5, inherit.aes = FALSE, fill = "transparent") +
  geom_sf(aes(fill = both), linewidth = .1) +
  ggthemes::theme_map() + scale_fill_discrete(na.value = "transparent") + ggtitle("Coverage > 1.05")

exp_counts <- harv_counts |> transmute(st, county_fips, harv_dr = sum_DR) |>
  full_join(medsl_count |> transmute(mit_dr = sum_DR, county_fips, st)) |>
  left_join(validation |> transmute(county_fips, val_dr = val_DR))

write.csv(exp_counts, "data/tmp_harv-medsl-total.csv")

