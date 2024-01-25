library(tidyverse)
library(haven)
library(arrow)
library(fs)

source("R/std_county_names.R")
datadir_snyder <- "~/Dropbox/CVR_Data_Shared/data_main/STATA_long/"

paths_snyder <- path(
  datadir_snyder, c("CA_Inyo_long.dta",
                    "CO_Arapahoe_long.dta",
                    "CO_Kit_Carson_long.dta",
                    "FL_Santa_Rosa_long.dta",
                    "GA_Cook_long.dta",
                    "GA_Grady_long.dta",
                    "GA_Laurens_long.dta",
                    "OH_Logan_long.dta",
                    "IL_Wayne_long.dta",
                    "NV_Eureka_long.dta"))
names(paths_snyder) <- fs::path_file(paths_snyder)

paths_json <- c(
  "~/Dropbox/CVR-JSON/NV/release/mineral_gen2020_offices_votes.dta"
)


snyder_item_all <- read_dta(path(datadir_snyder, "../item_choice_info.dta"))
snyder_item <- snyder_item_all |>
  select(state:dist, choice_id, party, level)

# Long, all ----
out_raw <- map(paths_snyder, read_dta, .progress = "read_dta") |>
  list_rbind(names_to = "filename")


out_cleaned <- out_raw |>
  separate_wider_delim(
    filename,
    delim = "_",
    too_many = "merge",
    names = c("state", "county")) |>
  # get state and county as displayed in Jim's data, then merge to get party
  mutate(county = str_remove(county, "_long.dta")) |>
  tidylog::left_join(
    snyder_item,
    by = c("state", "county", "column", "choice_id", "item")) |>
  rename(contest = item)


out_agg <- out_cleaned |>
  count(state, county, contest, dist, choice_id, choice, party, level)

counties <- read_csv("data-harvard/countycodes.csv") |>
  select(-state) |>
  rename(state = st)

out_off <- out_cleaned |>
  tidylog::filter(str_detect(contest, "^(US|ST)_(PRES|REP|SEN)$")) |>
  mutate(county_name = std_county_name(county), county = NULL) |>
  left_join(counties, by = c("state", "county_name")) |>
  relocate(state, county_fips, county_name)

out_off_sel <- out_off |>
  select(state, county_name, cvr_id, contest, dist, choice, party) |>
  mutate(across(where(is.character), as_factor))


write_feather(out_off, "data-harvard/samp-release_full.feather")
bench::mark(
  write_feather(out_off_sel, "data-harvard/samp-release.feather"),
  write_csv(out_off_sel, "data-harvard/samp-release.csv.gz"),
  write_dta(out_off_sel, "data-harvard/samp-release.dta")
)

# feather: write 0.5s, read 0.5s,   63.0MB chr 10MB fct
# csv.gz:  write 1.2s, read 1.0s,    6.4MB chr  4MB fct
# dta:     write 2.2s, read 5.6s   177.0MB chr 75MB fct


bench::mark(
  read_feather("data-harvard/samp-release.feather"),
  read_csv("data-harvard/samp-release.csv.gz"),
  read_dta("data-harvard/samp-release.dta"),
  check = FALSE
)

# wider check ----
out_wid <- out_off_sel |>
  mutate(contest = str_remove_all(contest, "_")) |>
  tidylog::distinct(state, county, cvr_id, contest, .keep_all = TRUE) |>
  pivot_wider(id_cols = c(state, county, cvr_id),
              names_from = contest,
              values_from = c(party, dist),
              names_glue = "{contest}_{.value}") |>
  select(-USPRES_dist) |>
  relocate(state:cvr_id, matches("dist"))


write_feather(out_wid, "data-harvard/samp-release-wide.feather")


#
