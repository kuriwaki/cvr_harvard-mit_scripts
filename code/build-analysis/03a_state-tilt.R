library(tidyverse)
library(scales)
library(arrow)
library(gt)
library(glue)
source("00_paths.R")

# Database connections
ds <- open_dataset(path(PATH_parq, "release")) |>
  filter(office == "US PRESIDENT")
ds_v <- open_dataset(path(PATH_parq, "returns/by-county")) |>
  filter(office == "US PRESIDENT")


#' Tally CVRs
tally_cvrs <- function(tbl) {
  tbl |>
    summarize(
      dem_prez = sum(party_detailed == "DEMOCRAT", na.rm = TRUE) /
        sum(party_detailed %in% c("DEMOCRAT", "REPUBLICAN"), na.rm = TRUE),
      n_counties = n_distinct(county_name),
      n_voters = sum(!candidate %in% c("OVERVOTE", "UNDERVOTE"), na.rm = TRUE),
    ) |>
    collect()
}

#' Tally Biden votes
tally_votes <- function(tbl) {
  tbl |>
    summarize(
      dem_prez = sum(votes*as.numeric(party_detailed == "DEMOCRAT"), na.rm = TRUE) /
        sum(votes*as.numeric(party_detailed %in% c("DEMOCRAT", "REPUBLICAN")), na.rm = TRUE),
      n_counties = n_distinct(county_name),
      n_voters = sum(votes*(as.numeric(!candidate %in% c("OVERVOTE", "UNDERVOTE"))), na.rm = TRUE)
    ) |>
    collect()
}


# CVR side ---
N_states <- count(ds, state) |> collect() |> nrow()
N_st_txt <- glue("All {N_states} states")

allstates_cvr <- ds |>
  tally_cvrs() |>
  mutate(state = N_st_txt)

state_cvr <- ds |>
  group_by(state) |>
  tally_cvrs() |>
  arrange(state) |>
  bind_rows(allstates_cvr)


# Population ---
# all 50 states
all_states_v <- ds_v |>
  tally_votes() |>
  mutate(state = "All 50 States")

# what about for states in our data
our_states_v <- ds_v |>
  semi_join(count(ds, state), by = "state") |>
  tally_votes() |>
  mutate(state = N_st_txt)

# state by state
state_info <- ds_v |>
  filter(office == "US PRESIDENT") |>
  group_by(state) |>
  tally_votes() |>
  arrange(state) |>
  bind_rows(our_states_v) |>
  bind_rows(all_states_v)


# Merge and create Table 3
state_cvr |>
  add_row(state = "All 50 States") |>
  left_join(state_info, by = c("state")) |>
  select(-matches("counties")) |>
  mutate(
    state = str_to_title(state),
    net_diff_prez = dem_prez.x - dem_prez.y,
    net_diff_voters = n_voters.x - n_voters.y) |>
  gt() |>
  fmt_percent(matches("_prez"), decimals = 1) |>
  fmt_percent(matches("net_diff_prez"), decimals = 2) |>
  fmt_integer(matches("voters")) |>
  cols_label_with(fn = \(x) case_when(x == "state" ~ "State", str_detect(x, "\\.x$") ~ "CVR", str_detect(x, "\\.y$") ~ "Pop.", str_detect(x,"^net_diff") ~ "Diff.")) |>
  tab_spanner("% Biden", columns = matches("_prez")) |>
  tab_spanner("Voters", columns = matches("voters")) |>
  gt::sub_missing() |>
  gtsave("tables/table_03.docx")
