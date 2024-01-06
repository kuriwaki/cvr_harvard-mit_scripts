library(tidyverse)
library(arrow)
library(ccesMRPprep)
library(gt)

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

# Compare ---

# US House
sum_ushou <- function(tbl) {
  tbl |>
  filter(office == "US_REP") |>
    tidylog::filter(!is.na(dist)) |>
    tidylog::filter(!is.na(st)) |>
    count(st, dist, candidate, party, wt = N) |>
    mutate(dist = str_remove(dist, "^0+")) |>
    mutate(cd = ccesMRPprep::to_cd(st, dist)) |>
    filter(str_detect(cd, "NA", negate = TRUE)) |>
    summarize(
      N_REP = sum(n * (party == "REP"), na.rm = TRUE),
      N_DEM = sum(n * (party == "DEM"), na.rm = TRUE),
      .by = c(cd)) |>
    tidylog::filter(N_REP > 0 | N_DEM > 0)
}

# Table ---
comp_tbl_ush <- full_join(
  medsl_counts |> sum_ushou(),
  harv_counts |> sum_ushou(),
  by = c("cd"),
  suffix = c("_medsl", "_harv")
) |>
  arrange(cd)

comp_tbl_ush |>
  gt() |>
  sub_missing() |>
  tab_spanner("MEDSL", columns = matches("_medsl")) |>
  tab_spanner("Harvard", columns = matches("_harv")) |>
  cols_label(N_REP_medsl = "R",
             N_REP_harv = "R",
             N_DEM_harv = "D",
             N_DEM_medsl = "D") |>
  fmt_integer() |>
  gt::as_raw_html() |>
  write_lines("data/tmp_ushou_harv-medsl_html.html")


comp_tbl_ush |>
  write_csv("data/tmp_ushou_harv-medsl.csv")
