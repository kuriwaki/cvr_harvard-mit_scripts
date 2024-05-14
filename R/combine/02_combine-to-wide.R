# create dataset like
# st office dist  candidate party votes_h votes_m
# AL ST_HOU  001 JOHN SMITH     R    1299    1300

library(tidyverse)
library(arrow)
library(fs)

username <- Sys.info()["user"]
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
}

# Data ----
dsa_h <- open_dataset(path(PATH_parq, "harvard"))
dsa_m <- open_dataset(path(PATH_parq, "medsl"))
dsa_v <- open_dataset(path(PATH_parq, "returns/by-county/"))

# Settings --
parties_use <- c("DEMOCRAT", "REPUBLICAN", "LIBERTARIAN", "GREEN",
                 "UNDERVOTE", "UNDERVOTES", "undervote",
                 "WRITEIN", "WRITE-IN")
offices_use <- c("US PRESIDENT", "US HOUSE", "US SENATE",
                 "STATE HOUSE", "STATE SENATE", "GOVERNOR")

office_simpl <- c("US PRESIDENT" = "uspres",
                  "US HOUSE" = "ushou",
                  "US SENATE"= "ussen",
                  "STATE HOUSE" = "sthou",
                  "STATE SENATE" = "stsen",
                  "GOVERNOR" = "stgov")

# counts -----

## Harvard
count_h <- dsa_h |>
  # TODO: do this beforehand
  filter(!(state == "ARIZONA" & office == "STATE HOUSE"),
         party_detailed %in% parties_use) |>
  count(state, county_name, office, district, candidate, party_detailed,
        name = "votes") |>
  collect() |>
  mutate(party_detailed = recode(party_detailed, "undervote" = "UNDERVOTE")) |>
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |>
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |>
  rename(candidate_h = candidate, votes_h = votes)

## MIT =-------
count_m <- dsa_m |>
  count(state, county_name, office, district,
        candidate, party_detailed, contest,
        name = "votes") |>
  collect() |>
  filter(!(state == "ARIZONA" & office == "STATE HOUSE")) |>
  mutate(
    county_name = replace(county_name, state %in% c("ALASKA", "RHODE ISLAND"), "STATEWIDE")
    ) |>
  # count across "context"
  count(state, county_name, office, district,
        candidate, party_detailed,
        name = "votes", wt = votes) |>
  # https://github.com/kuriwaki/CVR_Harvard-MIT/issues/15
  mutate(party_detailed = case_when(
    candidate == "UNDERVOTE" ~ "UNDERVOTE",
    candidate == "WRITEIN" ~ "WRITEIN",
    candidate == "OVERVOTE" ~ "OVERVOTE",
    .default = party_detailed)) |>
  filter(party_detailed %in% parties_use) |>
  # top-two
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |>
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |>
  rename(candidate_m = candidate, votes_m = votes)

# all counties that occur in one of H or M
all_counties <- full_join(
  count_m |> count(state, county_name, name = "count_m"),
  count_h |> count(state, county_name, name = "count_h")
) |> mutate(across(everything(), ~ replace_na(.x, 0)))

## Returns ------
count_v <- dsa_v |>
  filter(!(state == "ARIZONA" & office == "STATE HOUSE"),
         party_detailed %in% parties_use | (is.na(party_detailed) & office != "US PRESIDENT")) |>
  count(state, county_name, office, district, candidate, party_detailed, writein,
        special,
        wt = votes,
        name = "votes") |>
  collect() |>
  arrange(state, county_name, office, district, party_detailed, desc(votes)) |>
  mutate(cand_rank = 1:n(), .by = c(state, office, district, party_detailed, county_name)) |>
  rename(candidate_v = candidate, votes_v = votes) |>
  # ALL COUNTIES ever mentioned in H or M
  semi_join(all_counties, by = c("state", "county_name"))

# Classifications ---
colours <- read_csv(path(PATH_parq, "validation/classifications.csv"), show_col_types = FALSE)


# Together ------
joinvars <- c("state", "county_name", "office", "district", "party_detailed", "cand_rank")
out_cand <- count_h |>
  full_join(count_m, by = joinvars) |>
  full_join(count_v, by = joinvars) |>
  select(-cand_rank) |>
  arrange(state, county_name, desc(office), district, party_detailed) |>
  relocate(state:district, party_detailed, special, writein)

# one row per county
out_county <- out_cand |>
  filter(party_detailed %in% c("REPUBLICAN", "DEMOCRAT", "LIBERTARIAN")) |>
  summarize(votes_h = sum(votes_h, na.rm = TRUE),
            votes_m = sum(votes_m, na.rm = TRUE),
            votes_v = sum(votes_v, na.rm = TRUE),
            .by = c(state, county_name, office)) |>
  mutate(
    diff_h = votes_v - votes_h,
    diff_m = votes_v - votes_m,
    office = recode(office, !!!office_simpl)) |>
  pivot_wider(id_cols = c(state, county_name),
              names_from = c(office),
              values_from = matches("(votes|diff)_"),
              names_glue = "{office}_{.value}",
              names_vary = "slowest") |>
  mutate(match_score = rowMeans(pick(matches("diff")) == 0, na.rm = TRUE),
         match_score_h = rowMeans(pick(matches("diff_h")) == 0, na.rm = TRUE),
         match_score_m = rowMeans(pick(matches("diff_m")) == 0, na.rm = TRUE)
  ) |>
  # need to run this twice
  left_join(colours, by = c("state", "county_name" = "county")) |>
  relocate(state, county_name, colour,
           matches("match_"), matches("uspres"), matches("ushou"), matches("ussen"))

list(`by-county-district` = out_cand, `by-county` = out_county) |>
  writexl::write_xlsx(path(PATH_parq, "combined/compare.xlsx"))



# check ---

# counties that don't appear in validation (all should!)
anti_join(all_counties, count_v, by = c("state", "county_name"))

out_county |>
  mutate(match_h = as.integer(match_score_h == 1),
         match_m = as.integer(match_score_m == 1)) |>
  xtabs(~ match_h + match_m, data = _) |>
  addmargins() |>
  kableExtra::kbl(format = "pipe",
                  caption = "Harvard exact match (rows) vs. MEDSL exact match (cols)") |>
  write_lines("status/by-county_correct-H-vs-M.txt")


out2 <- out_cand |>
  filter(party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |>
  summarize(
    across(starts_with("votes_"), sum),
    .by = c(state, county_name)) |>
  mutate(
    diff_h = votes_v - votes_h,
    diff_m  = votes_v - votes_m,
    diff_hm = votes_h - votes_m) |>
  summarize(
    agree_hm = all(diff_hm == 0, na.rm = TRUE),
    correct_h = all(diff_h == 0, na.rm = TRUE),
    correct_m = all(diff_m == 0, na.rm = TRUE),
    diff_h = sum(abs(votes_h - votes_v)),
    diff_m = sum(abs(votes_m - votes_v)),
    votes_v = sum(votes_v),
    n = n(),
    .by = c(state, county_name))

out2 |>
  tidylog::semi_join(
    filter(out_county, uspres_votes_m > 0, uspres_votes_h > 0),
    by = c("state", "county_name")) |>
  count(agree_hm)|>
  kableExtra::kbl(format = "pipe") |>
  write_lines("status/by-county_H-M-agreement.txt")

