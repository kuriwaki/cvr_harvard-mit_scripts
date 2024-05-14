#' Extract Jeff's precinct results
username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_snyder <- "~/Dropbox/CVR_Data_Shared/data_main"
  PATH_parq <- "~/Dropbox/CVR_parquet"
}

library(tidyverse)
library(writexl)
library(fs)

# Inputs ----
dat <- read_csv(path(PATH_snyder, "to-parquet/JBL/precinct_merge_summary.csv"))
colors <- read_csv(path(PATH_parq, "validation/classifications.csv"))

# Combine ----
out <- dat |>
  mutate(
    state = case_match(
      state,
      "AZ" ~ "ARIZONA",
      "AK" ~ "ALASKA",
      "CA" ~ "CALIFORNIA",
      "CO" ~ "COLORADO",
      "FL" ~ "FLORIDA",
      "GA" ~ "GEORGIA",
      "IL" ~ "ILLINOIS",
      "MD" ~ "MARYLAND",
      "MI" ~ "MICHIGAN",
      "NJ" ~ "NEW JERSEY",
      "NM" ~ "NEW MEXICO",
      "NV" ~ "NEVADA",
      "OH" ~ "OHIO",
      "OR" ~ "OREGON",
      "RI" ~ "RHODE ISLAND",
      "TN" ~ "TENNESSEE",
      "TX" ~ "TEXAS",
      "WV" ~ "WEST VIRGINIA",
      "WI" ~ "WISCONSIN",
      "UT" ~ "UTAH"
    ),
    county = str_to_upper(county),
  ) |>
  select(-n_precincts_sov) |>
  mutate(across(where(is.double), \(x) round(x, 3))) |>
  tidylog::left_join(colors, by = c("state", "county" = "county")) |>
  relocate(state, county, colour)

# Write ----
out |>
  write_xlsx(path(PATH_parq, "combined/precincts_match.xlsx"))


# summary of results -- log for repo ----
my_summ <- function(tbl) {
  tbl |>
    summarize(
      match_within_prec = sum(max_vote_dist == 0),
      n_counties = n(),
      n_states = n_distinct(state)
    )
}

all_rows <- my_summ(out) |>
  mutate(colour = "All counties")

out |>
  group_by(colour) |>
  my_summ() |>
  ungroup() |>
  add_row(all_rows) |>
  mutate(colour = fct_relevel(colour, "green", "yellow", "red")) |>
  arrange(colour) |>
  kableExtra::kbl(format = "pipe") |>
  write_lines("status/precinct-match.txt")
