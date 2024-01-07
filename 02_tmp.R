library(tidyverse)
library(arrow)
library(ccesMRPprep)
library(dataverse)
library(gt)

countycodes <- read_csv("data/countycodes.csv")
parq_orig <- open_dataset("data/MEDSL/cvrs_statewide/")
harv_counts_raw <- read_csv("data/cand-counts_harvard.csv")
elec_results <- get_dataframe_by_name(
  "candidates_2006-2020.tab",
  server = "dataverse.harvard.edu",
  dataset = "10.7910/DVN/DGDRDT",
  original = TRUE,
  .f = haven::read_dta
)

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

elec_wide <- elec_results |>
  filter(office == "H", year == 2020) |>
  mutate(cd = to_cd(state, dist)) |>
  summarize(
    N_R_all = sum(candidatevotes * (party == "R"), na.rm = TRUE),
    N_D_all = sum(candidatevotes * (party == "D"), na.rm = TRUE),
    .by = cd
  ) |>
  transmute(
    cd,
    shareD_all = N_D_all/(N_D_all + N_R_all),
    N_all = N_R_all + N_D_all,
  )

# Table ---
comp_tbl_ush <- full_join(
  medsl_counts |> sum_ushou(),
  harv_counts |> sum_ushou(),
  by = c("cd"),
  suffix = c("_medsl", "_harv")
) |>
  transmute(
    cd,
    shareD_medsl = N_DEM_medsl/(N_DEM_medsl + N_REP_medsl),
    shareD_harv = N_DEM_harv/(N_DEM_harv + N_REP_harv),
    N_medsl = N_REP_medsl + N_DEM_medsl,
    N_harv = N_REP_harv + N_DEM_harv) |>
  left_join(elec_wide, by = "cd") |>
  arrange(cd) |>
  relocate(cd, matches("N_"), matches("shareD_"))

comp_tbl_ush |>
  gt() |>
  sub_missing() |>
  tab_spanner("US House Votes (R + D)", columns = matches("N_")) |>
  tab_spanner("% Dem", columns = matches("shareD_")) |>
  cols_label_with(
    fn =
    \(x) case_when(
      str_detect(x, "harv") ~ "Harv",
      str_detect(x, "medsl") ~ "MIT",
      str_detect(x, "_all") ~ "Pop",
      x == "cd" ~ "CD"
      )
  ) |>
  fmt_integer(starts_with("N_")) |>
  fmt_number(starts_with("shareD_"), decimals = 2)

comp_tbl_ush |>
  write_csv("data/tmp_ushou_harv-medsl.csv")
