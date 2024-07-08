# create dataset like
# state       office dist  candidate  party votes_h votes_m
# ALASKA STATE HOUSE  001 JOHN SMITH    REP    1299    1300

suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(fs)
})

source("R/combine/01c_classification-R.R")

username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

# Data ----
dsa_h <- open_dataset(path(PATH_parq, "harvard"))
dsa_m <- open_dataset(path(PATH_parq, "medsl"))
dsa_c <- open_dataset(path(PATH_parq, "intermediate/coalesced"))
dsa_v <- open_dataset(path(PATH_parq, "returns/by-county"))

# Settings --
parties_use <- c("DEMOCRAT", "REPUBLICAN", "LIBERTARIAN", "GREEN",
                 "NO PARTY AFFILIATION",
                 "UNDERVOTE", "UNDERVOTES", "undervote",
                 "WRITEIN", "WRITE-IN")
offices_use <- c("US PRESIDENT", "US HOUSE", "US SENATE",
                 "STATE HOUSE", "STATE SENATE", "GOVERNOR")

office_simpl <- c("US PRESIDENT" = "uspres",
                  "US SENATE"= "ussen",
                  "US HOUSE" = "ushou",
                  "STATE SENATE" = "stsen",
                  "STATE HOUSE" = "sthou",
                  "GOVERNOR" = "stgov")

# counts -----

## Harvard
count_h <- dsa_h |>
  # TODO: do this beforehand
  filter(party_detailed %in% c(parties_use, "NPA")) |>
  count(state, county_name, office, district, candidate, party_detailed,
        name = "votes") |>
  collect() |>
  mutate(party_detailed = recode(party_detailed, "undervote" = "UNDERVOTE",
                                 NPA = "NO PARTY AFFILIATION")) |>
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |>
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |>
  rename(candidate_h = candidate, votes_h = votes)


## MIT =-------
summ_fmt <- function(tbl) {
  tbl |>
    # fix Thomas Hall
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/174
    mutate(
      candidate = ifelse(
        state == "OHIO" & office == "STATE HOUSE" & district == "053" & candidate == "THOMAS HELL",
        "THOMAS HALL", candidate),
    ) |>
    # fix district in Mason, there is only one district and medsl has it wrong
    mutate(district = ifelse(state == "MICHIGAN" & county_name == "MASON" & district == "103", "101", district)) |>
    # Add missing party affiliations
    left_join(read_delim("R/combine/metadata/missing-party-metadata.txt", delim = ",", col_types = "ccccci"),
              by = c("state", "office", "candidate", "district"),
              relationship = "many-to-one") |>
    mutate(party_detailed = coalesce(party_detailed.x, party_detailed.y),
           party_detailed.x = NULL, party_detailed.y = NULL) |>
    count(state, county_name, office, district,
          candidate, party_detailed, contest,
          name = "votes") |>
    collect() |>
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/41
    filter(state != "VIRGINIA") |>
    mutate(
      county_name = replace(county_name, state %in% c("ALASKA", "DELAWARE", "RHODE ISLAND"), "STATEWIDE")
    ) |>
    # count across "context"
    count(state, county_name, office, district,
          candidate, party_detailed,
          name = "votes", wt = votes) |>
    # For merging purposes, use undervote as a party
    mutate(party_detailed = case_when(
      candidate == "UNDERVOTE" ~ "UNDERVOTE",
      candidate == "WRITEIN" ~ "WRITEIN",
      candidate == "OVERVOTE" ~ "OVERVOTE",
      .default = party_detailed)) |>
    filter(party_detailed %in% parties_use) |>
    # top-two
    arrange(state, county_name, office, district, party_detailed, desc(votes)) |>
    mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name))
}

count_m <- dsa_m |> summ_fmt() |> rename(candidate_m = candidate, votes_m = votes)
count_c <- dsa_c |> summ_fmt() |> rename(candidate_c = candidate, votes_c = votes)


## Returns ------
# all counties that occur in one of H or M
all_counties <- full_join(
  count_m |> count(state, county_name, name = "count_m"),
  count_h |> count(state, county_name, name = "count_h"),
  by = c("state", "county_name"),
  relationship = "one-to-one"
)

count_v <- dsa_v |>
  filter(party_detailed %in% parties_use) |>
  count(state, county_name, office, district, candidate, party_detailed, writein,
        special,
        wt = votes,
        name = "votes") |>
  collect() |>
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |>
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |>
  tidylog::filter(votes > 0) |>
  rename(candidate_v = candidate, votes_v = votes) |>
  # ALL COUNTIES ever mentioned in H or M
  semi_join(all_counties, by = c("state", "county_name"))

# Classifications ---
precs_all <- readxl::read_excel(path(PATH_parq, "combined/precincts_match.xlsx"))
precs <- precs_all |>
  select(state, county, n_precincts_cvr, n_precincts_vest,
         max_precinct_uspres_diff = max_vote_dist) |>
  filter(state != "ALASKA") |>
  arrange(state)

# Together ------
joinvars <- c("state", "county_name", "office", "district", "party_detailed", "cand_rank")
out_cand <- count_h |>
  full_join(count_m, by = joinvars) |>
  left_join(count_c, by = joinvars) |>
  full_join(count_v, by = joinvars) |>
  select(-cand_rank) |>
  mutate(office = factor(office, levels = names(office_simpl))) |>
  arrange(state, county_name, office, district, party_detailed) |>
  relocate(state:district, party_detailed, special, writein) |>
  anti_join(read_csv("R/release/metadata/counties_remove.csv", col_types = "cc"),
            by = c("state", "county_name"))

cand_summ_h <- categorize_diff(out_cand, votes_h, color2_h, candidate_h)
cand_summ_m <- categorize_diff(out_cand, votes_m, color2_m, candidate_m)
cand_summ_c <- categorize_diff(out_cand, votes_c, color2_c, candidate_c)

# one row per county
out_county <- out_cand |>
  filter(party_detailed %in% c("REPUBLICAN", "DEMOCRAT", "LIBERTARIAN")) |>
  summarize(votes_h = sum(votes_h, na.rm = TRUE),
            votes_m = sum(votes_m, na.rm = TRUE),
            votes_c = sum(votes_c, na.rm = TRUE),
            votes_v = sum(votes_v, na.rm = TRUE),
            .by = c(state, county_name, office)) |>
  mutate(
    diff_h = votes_v - votes_h,
    diff_m = votes_v - votes_m,
    diff_c = votes_v - votes_c,
    office = recode(office, !!!office_simpl)) |>
  pivot_wider(id_cols = c(state, county_name),
              names_from = c(office),
              values_from = matches("(votes|diff)_"),
              names_glue = "{office}_{.value}",
              names_vary = "slowest") |>
  mutate(match_score_h = rowMeans(pick(matches("diff_h")) == 0, na.rm = TRUE),
         match_score_m = rowMeans(pick(matches("diff_m")) == 0, na.rm = TRUE)
  ) |>
  left_join(precs, by = c("state", "county_name" = "county")) |>
  left_join(cand_summ_h, by = c("state", "county_name")) |>
  left_join(cand_summ_m, by = c("state", "county_name")) |>
  # Declare release criterion ----
  left_join(cand_summ_c, by = c("state", "county_name")) |>
  mutate(release = as.integer(color2_c %in% c("any < 1% mismatch", "0 difference"))) |>
  relocate(state, county_name,
           matches("color2"),
           release,
           matches("match_"),
           matches("precinct"),
           matches("uspres"), matches("ushou"), matches("ussen"))

release_counties <-  out_county |>
  distinct(state, county_name, release)

out_coal <- out_cand |>
  left_join(release_counties, relationship = "many-to-one", by = c("state", "county_name")) |>
  select(state:party_detailed, release, matches("_(c|v)$")) |>
  tidylog::filter(any(!is.na(votes_c)), .by = c(state, county_name)) |>
  mutate(diff_pct = scales::comma(((votes_v - votes_c) / votes_v), accuracy = 0.001),
         diff_pct = replace(diff_pct, !party_detailed %in% c("REPUBLICAN", "DEMOCRAT", "LIBERTARIAN", "GREEN"), NA))


# Write to Dropbox -----
list(`by-cand` = select(out_cand, !matches("_c$")),
     `by-county` = select(out_county, !matches("(diff|votes)_c$")),
     `by-cand-coalesced` = out_coal,
     `precinct` = precs_all) |>
  writexl::write_xlsx(path(PATH_parq, "combined/compare.xlsx"))


# check ----

# counties that don't appear in validation (all should!)
anti_join(all_counties, count_v, by = c("state", "county_name"))

## marginal error rate
print_tab <- function(tbl, var, path) {
  tbl |>
    count({{var}}) |>
    kableExtra::kbl(format = "pipe") |>
    write_lines(path)
}
out_county |> print_tab(color2_h, "status/colors2_h.txt")
out_county |> print_tab(color2_m, "status/colors2_m.txt")
out_county |> print_tab(color2_c, "status/colors2_c.txt")

## 2 by 2 comparisons
out_county |>
  mutate(match_h = as.integer(match_score_h == 1),
         match_m = as.integer(match_score_m == 1)) |>
  xtabs(~ match_h + match_m, data = _) |>
  addmargins() |>
  kableExtra::kbl(format = "pipe",
                  caption = "Harvard exact match (rows) vs. MEDSL exact match (cols)") |>
  write_lines("status/by-county_correct-H-vs-M.txt")



# Overall classification (old "color") ----
library(reticulate)

virtualenv_create(packages = c("openpyxl", "pandas")) # set force = TRUE once
use_virtualenv("~/.virtualenvs/r-reticulate")
py_config()
source_python("R/combine/01a_gen_classifications.py", envir = NULL)
