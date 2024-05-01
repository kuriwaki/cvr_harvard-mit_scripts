# Clean validation dataset from MEDSL to our format
library(tidyverse)
library(arrow)
library(fs)


username <- Sys.info()["user"]

# other users should make a different clause
if (username %in% c("shirokuriwaki", "sk2983")) {
  path_source <- "~/Dropbox/CVR_parquet/returns/raw/precincts_20240429.zip" # only works for shirokuriwaki
  path_outdir <- "~/Dropbox/CVR_parquet"
}


# Read whole data -----
## from @sbaltzmit
tictoc::tic()
ret_all <- read_csv(
  file = path_source,
  col_types = "ccccciciccccciccllciiiDli")
tictoc::toc()

statewide = c("ALASKA", "RHODE ISLAND")

# only the top six offices -----
## most of data reformatting
ret_sel <- ret_all |>
  tidylog::filter(office %in% c("US PRESIDENT", "US HOUSE", "US SENATE",
                                "STATE HOUSE", "STATE SENATE", "GOVERNOR")) |>
  mutate(jurisdiction_name = replace(jurisdiction_name, state %in% statewide, NA)) |>
  mutate(jurisdiction_fips = replace(jurisdiction_fips, state %in% statewide, NA),
         county_name = replace(county_name, state %in% statewide, "STATEWIDE")) |>
  arrange(state_fips, county_fips) |>
  select(state,
         matches("county"),
         matches("jurisdiction"),
         precinct,
         mode,
         office,
         district,
         magnitude,
         special,
         writein,
         matches("party"),
         candidate,
         votes
  ) |>
  # to match to Harvard on party
  mutate(writein = as.integer(writein),
         special = as.integer(special),
         party_detailed = case_when(candidate == "UNDERVOTES" ~ "UNDERVOTE",
                                    candidate == "WRITEIN" ~ "WRITEIN",
                                    candidate == "OVERVOTES" ~ "OVERVOTE",
                                    .default = party_detailed)) |>
  # replace statewide office's district with
  mutate(district = replace(district, office == "US PRESIDENT", "FEDERAL")) |>
  mutate(dist_state = replace(state, !office %in% c("GOVERNOR", "US SENATE"), NA),
         district = coalesce(dist_state, district),
         district = str_pad(district, width = 3, pad = "0")) |> # ALASKA ST SEN needs padding
  mutate(district = replace(district, state == "GEORGIA" & special, "GEORGIA-III")) |>
  tidylog::mutate(party_detailed = replace(party_detailed, candidate == "ALLEN BUCKLEY" & state == "GEORGIA", "INDEPENDENT"),
                  party_simplified = replace(party_detailed, candidate == "ALLEN BUCKLEY" & state == "GEORGIA", "OTHER"))


# sum by county x mode ------
by_vars <- c("state", "county_name", "county_fips", "jurisdiction_name",
             "jurisdiction_fips",  "office", "district",
             "magnitude",  "special", "writein",
             "mode",
             "party_detailed", "party_simplified", "candidate")

county_mode_summ <- ret_sel |>
  summarize(
    votes = sum(votes, na.rm = TRUE),
    .by = by_vars
  )

# sum by county
county_summ <- county_mode_summ |>
  summarize(
    votes = sum(votes, na.rm = TRUE),
    .by = setdiff(by_vars, "mode")
  )


# write to parquet ---
write_pq <- function(obj, pq_name, dir = path_outdir) {
  obj |>
    write_dataset(
      fs::path(dir, pq_name),
      format = "parquet",
      existing_data_behavior = "delete_matching"
    )
}

write_pq(ret_sel,          "returns/by-precinct-mode")
write_pq(county_summ,      "returns/by-county")
write_pq(county_mode_summ, "returns/by-county-mode")
