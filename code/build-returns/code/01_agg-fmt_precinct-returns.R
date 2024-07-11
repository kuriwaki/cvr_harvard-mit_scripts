# Clean validation dataset from MEDSL to our format
suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(dataverse)
  library(fs)
})

username <- Sys.info()["user"]

# other users should make a different clause
if (username == "shirokuriwaki" | str_detect(username, "^sk[0-9]+")) {
  path_source <- "~/Dropbox/CVR_parquet/returns/raw/precincts_20240429.zip"
  path_outdir <- "~/Dropbox/CVR_parquet"
} else if (username == "mason") {
  path_source <- "~/Dropbox (MIT)/Research/CVR_parquet/returns/raw/precincts_20240429.zip"
  path_outdir <- "~/Dropbox (MIT)/Research/CVR_parquet/"
}


# Read whole data -----
ret_all <- read_csv(file = path_source, col_types = "cccccnciccccciccllciiiDli")

statewide <- c("ALASKA", "RHODE ISLAND", "DELAWARE")

# Custom adds ------
# Oregon substitute (must be V5 or above)
ret_oregon <- get_dataframe_by_name(
  "2020-or-precinct-general.tab",
  dataset = "10.7910/DVN/NT66Z3",
  server = "dataverse.harvard.edu",
  original = TRUE,
  .f = read_csv
) |>
  mutate(jurisdiction_fips = as.character(jurisdiction_fips))

ret_adds <- read_csv(
  path("metadata", "manual_additions.csv"),
  col_types = "ccccicccc"
) |>
  mutate(
    mode = "TOTAL",
    jurisdiction_name = county_name
  )

ret_nv_totals <- read_csv(
  path("metadata", "nv_res_complete.csv")
) |>
  select(
    state,
    county_name, county_fips,
    office, district, candidate,
    candidatevotes
  )

fl_sh096_agg <- tibble(
  state = "FLORIDA",
  county_name = "BROWARD",
  county_fips = 12011,
  jurisdiction_name = "BROWARD",
  jurisdiction_fips = "12011",
  office = "STATE HOUSE",
  district = "096",
  magnitude = 1,
  special = 0,
  writein = 0,
  party_detailed = c("DEMOCRAT"),
  party_simplified = c("DEMOCRAT"),
  candidate = c("CHRISTINE HUNSCHOFSKY"),
  votes = 66892
)

fl_npas <-
  tribble(
    ~office, ~district, ~candidate,
    "STATE HOUSE", "062", "LAURIE RODRIGUEZ-PERSON",
    "STATE HOUSE", "088", "RUBIN ANDERSON",
    "STATE SENATE", "009", "JESTINE IANNOTTI",
    "STATE SENATE", "019", "CHRISTINA PAYLAN",
    "STATE SENATE", "023", "ROBERT KAPLAN",
    "STATE SENATE", "037", "ALEX RODRIGUEZ",
    "STATE SENATE", "039", "CELSO D ALFONSO",
    "US HOUSE", "001", "ALBERT ORAM",
    "US HOUSE", "017", "THEODORE \"PINK TIE\" MURRAY",
    "US HOUSE", "018", "K W MILLER",
    "US HOUSE", "021", "CHARLESTON MALKEMUS",
    "US HOUSE", "024", "CHRISTINE ALEXANDRIA OLIVO",
  ) |>
  mutate(
    state = "FLORIDA",
    party_detailed = "NO PARTY AFFILIATION"
  )

# only the top six offices -----
## most of data reformatting
ret_sel <- ret_all |>
  # add Oregon
  filter(state != "OREGON") |>
  bind_rows(ret_oregon) |>
  bind_rows(ret_adds) |>
  tidylog::filter(office %in% c(
    "US PRESIDENT", "US HOUSE", "US SENATE",
    "STATE HOUSE", "STATE SENATE", "GOVERNOR"
  )) |>
  mutate(
    jurisdiction_name = replace(jurisdiction_name, state %in% statewide, NA),
    jurisdiction_fips = replace(jurisdiction_fips, state %in% statewide, NA),
    county_name = replace(county_name, state %in% statewide, "STATEWIDE")
  ) |>
  arrange(state_fips, county_fips) |>
  select(
    state, matches("county"), matches("jurisdiction"),
    precinct, mode, office, district, magnitude,
    special, writein, matches("party"), candidate, votes
  ) |>
  mutate(
    writein = as.integer(writein),
    special = as.integer(special),
    # to match to Harvard on party
    party_detailed = case_when(candidate == "UNDERVOTES" ~ "UNDERVOTE",
      candidate == "WRITEIN" ~ "WRITEIN",
      candidate == "OVERVOTES" ~ "OVERVOTE",
      .default = party_detailed
    ),
    # convert to statewide format
    district = replace(district, office == "US PRESIDENT", "FEDERAL"),
    dist_state = replace(state, !office %in% c("GOVERNOR", "US SENATE"), NA),
    district = coalesce(dist_state, district),
    # ALASKA ST SEN needs padding
    district = str_pad(district, width = 3, pad = "0"),
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/24
    district = replace(district, state == "GEORGIA" & special, "GEORGIA-III"),
    # sometimes a precinct is formatted differently by office; unify
    precinct = if_else(state == "TEXAS" & county_name %in% c("BOSQUE", "COLLIN"),
      str_pad(precinct, width = 7, pad = "0"),
      precinct
    ),
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/29
    party_detailed = replace(party_detailed, candidate == "ALLEN BUCKLEY" & state == "GEORGIA", "INDEPENDENT"),
    party_simplified = replace(party_detailed, candidate == "ALLEN BUCKLEY" & state == "GEORGIA", "OTHER"),
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/181
    party_detailed = replace(party_detailed, candidate == "HOWIE HAWKINS" & state == "WEST VIRGINIA", "GREEN"),
    party_simplified = replace(party_detailed, candidate == "HOWIE HAWKINS" & state == "WEST VIRGINIA", "GREEN"),
    # https://github.com/kuriwaki/cvr_harvard-mit_scripts/issues/33
    across(matches("party_"), \(x) case_match(x, "DEMOCRATIC FARMER LABOR" ~ "DEMOCRAT", .default = x)),
    writein = ifelse(state == "WISCONSIN" & office == "STATE HOUSE" & candidate == "STEVE KUNDERT", 1, writein)
  ) |>
  # in Utah and two counties in NY, there is a "total" as well as a non-total entry, double counting votes.
  # If there is a non-total AND total in the set, then drop all the non-total votes (in UT, they seem to be 0 votes)
  tidylog::filter(!(any(mode == "TOTAL") & mode != "TOTAL"),
    .by = c(state, county_name, precinct, party_detailed, writein, special)
  ) |>
  tidylog::left_join(fl_npas, by = c("state", "office", "candidate", "district")) |>
  mutate(
    party_detailed = coalesce(party_detailed.x, party_detailed.y),
    party_detailed.x = NULL, party_detailed.y = NULL
  )

# sum by county x mode ------
by_vars <- c(
  "state", "county_name", "county_fips", "jurisdiction_name",
  "jurisdiction_fips", "office", "district",
  "magnitude", "special", "writein", "mode",
  "party_detailed", "party_simplified", "candidate"
)

county_mode_summ <- ret_sel |>
  summarize(
    votes = sum(votes, na.rm = TRUE),
    .by = all_of(by_vars)
  )

# sum by county
county_summ <- county_mode_summ |>
  summarize(
    votes = sum(votes, na.rm = TRUE),
    .by = all_of(setdiff(by_vars, "mode"))
  ) |>
  bind_rows(fl_sh096_agg) |>
  left_join(ret_nv_totals,
    by = c("state", "county_name", "county_fips", "office", "district", "candidate"),
    relationship = "one-to-one"
  ) |>
  # update with unredacted counts
  mutate(
    votes = coalesce(candidatevotes, votes),
    candidatevotes = NULL
  )


# write to parquet ---
write_pq <- function(obj, pq_name, dir = path_outdir) {
  obj |>
    write_dataset(
      path(dir, pq_name),
      format = "parquet",
      existing_data_behavior = "delete_matching"
    )
}

write_pq(ret_sel, "returns/by-precinct-mode")
write_pq(county_summ, "returns/by-county")
write_pq(county_mode_summ, "returns/by-county-mode")
