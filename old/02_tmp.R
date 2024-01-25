library(tidyverse)
library(arrow)
library(ccesMRPprep)
library(dataverse)
library(gt)

# Source
source("R/sum_cand-counts.R")


# Data ----
countycodes <- read_csv("data/countycodes.csv")
parq_orig <- open_dataset("data/MEDSL/cvrs_statewide/")
harv_counts_raw <- read_csv("data/cand-counts_harvard.csv")

counts_medsl_raw <- parq_orig |>
  filter(!office %in% "GOVERNOR") |>
  count(state, county_name, office, district, candidate, party_detailed,
        name = "N") |>
  collect()


medsl_counts <- counts_medsl_raw |>
  mutate(office = recode(
    office,
    `STATE HOUSE` = "ST_REP",
    `STATE SENATE` = "ST_SEN",
    `US HOUSE` = "US_REP",
    `US SENATE` = "US_SEN",
    `US PRESIDENT` = "US_PRES",
  )) |>
  left_join(countycodes |> mutate(state = str_to_upper(state)),
            by = c("county_name", "state")) |>
  mutate(party = recode(
    party_detailed,
    "DEMOCRAT" = "DEM",
    "REPUBLICAN" = "REP",
    "INDEPENDENT" = "IND",
    "NONPARTISAN" = "NPA",
    "LIBERTARIAN" = "LBT",
    "NA" = NA_character_,
    .default = party_detailed)) |>
  rename(dist = district) |>
  relocate(st, county_name, county_fips) |>
  select(-state)


harv_counts <- harv_counts_raw |>
  rename(N = n, office = contest)


# Table ---

fmt_comp_tbl <- function(tbl, id_var, elecdata) {
  tbl |>
    transmute(
      "{{id_var}}" := {{id_var}},
      shareD_medsl = N_DEM_medsl/(N_DEM_medsl + N_REP_medsl),
      shareD_harv = N_DEM_harv/(N_DEM_harv + N_REP_harv),
      N_harv = N_REP_harv + N_DEM_harv,
      N_medsl = N_REP_medsl + N_DEM_medsl) |>
    left_join(elecdata) |>
    arrange({{id_var}}) |>
    relocate({{id_var}}, matches("N_"), matches("shareD_"))
}

comp_tbl_ush <- full_join(
  medsl_counts |> sum_ushou(),
  harv_counts |> sum_ushou(),
  by = c("cd"),
  suffix = c("_medsl", "_harv")
) |>
  fmt_comp_tbl(cd, elecdata = elec_wide_H)

comp_tbl_uss <- full_join(
  medsl_counts |> sum_ussen(),
  harv_counts |> sum_ussen(),
  by = c("st"),
  suffix = c("_medsl", "_harv")
) |>
  fmt_comp_tbl(st, elecdata = elec_wide_S)


# Format table ----
fmt_gttbl <- function(tbl) {
  tbl |>
  gt() |>
    sub_missing() |>
    tab_spanner("Votes (R + D)", columns = matches("N_")) |>
    tab_spanner("% Dem", columns = matches("shareD_")) |>
    cols_label_with(
      fn =
        \(x) case_when(
          str_detect(x, "harv") ~ "Harv",
          str_detect(x, "medsl") ~ "MIT",
          str_detect(x, "_all") ~ "Pop",
          x == "cd" ~ "CD",
          x == "st" ~ "State"
        )
    ) |>
    fmt_integer(starts_with("N_")) |>
    fmt_number(starts_with("shareD_"), decimals = 2)
}

comp_tbl_ush |>
  select(-office) |>
  fmt_gttbl()

comp_tbl_uss |>
  select(-office) |>
  fmt_gttbl()

# TODO
# put tables into website
# make state and cd maps



# Write to table ---
comp_tbl_ush |>
  write_csv("data/tmp_ushou_harv-medsl.csv")
