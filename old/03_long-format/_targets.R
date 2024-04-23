# Load packages required to define the pipeline:
library(targets)

# tar_make(script = "03_long-format/_targets.R", store = "03_long-format/targets")

Sys.setenv(TAR_PROJECT = "03_long-format")

# Set target options:
tar_option_set(
  # packages that your targets need to run
  packages = c(
    "tibble",
    "arrow",
    "haven",
    "dplyr",
    "tidyr",
    "purrr",
    "readr",
    "stringr",
    "glue",
    "fs"
  ),
  format = "rds"
)

# Run the R scripts in the R/ folder with your custom functions:
source("R/std_county_names.R")

list(

  tar_target(countycodes,
             read_csv("data/countycodes.csv")),

  tar_target(medsl_parq,
             open_dataset("data/MEDSL/cvrs_statewide/")),

  tar_target(
    medsl_long,
    {
      medsl_long <- medsl_parq |> filter(is.na(voted) | voted == 1) |>
        mutate(candidate = str_trim(candidate)) |>
        count(office, candidate,
              district, county_name,
              state, party_detailed) |> collect()

      medsl_long <- medsl_long |>
        mutate(county_name = std_county_name(fix_county_name(county_name)),
               county_name = ifelse(state == "MARYLAND" & county_name == "ST. MARY", "ST. MARY'S", county_name),
               county_name = ifelse(state == "MICHIGAN" & county_name == "DICKENSON", "DICKINSON", county_name),
               county_name = ifelse(state == "ALASKA", "STATEWIDE", county_name)) |>
        left_join(countycodes |> select(state, county_name, county_fips) |> mutate(state = str_to_upper(state)))

      medsl_long |> select(-c(county_name))

    }
  ),

  tar_target(
    medsl_pres,

    medsl_long |>
      filter(office == "US PRESIDENT") |>
      filter(str_detect(candidate, "BIDEN|TRUMP")) |>
      mutate(party = ifelse(str_detect(candidate, "BIDEN"), "DEM", "REP")) |>
      select(-c(office, district))
  ),

  tar_target(
    harv_parq,
    open_dataset("data/harvard/cvrs_long/")
  ),

  tar_target(
    harv_long,
    harv_parq |>
      mutate(choice = str_trim(choice)) |>
      count(item, choice, dist, county, state, party) |>
      collect() |>
      mutate(county = fix_county_name(std_county_name(county))) |>
      rename(st = "state") |>
      left_join(countycodes, by = c("st" = "st", "county" = "county_name")) |>
      select(-c(st)) |> mutate(state = str_to_upper(state))
  )
 ,

 tar_target(
   harv_pres,
   harv_long |>
     filter(str_detect(choice, "BIDEN|TRUMP") & item == "US_PRES") |>
     select(-c(county, dist))
  ),

 tar_target(
   validation,
   read_csv("data/validation-pres.csv") |>
     mutate(party = ifelse(choice == "Biden", "DEM", "REP")) |>
     select(county_fips, votes_reported, party) |>
     mutate(item = "US_PRES") |>
     group_by(item, county_fips) |>
     summarise(
       valid_DR = sum(votes_reported),
       valid_D = sum(votes_reported * (party == "DEM")),
       valid_R = sum(votes_reported * (party == "REP")))
 ),

 tar_target(
   pres_summary,
   harv_pres |>
     summarise(
       harv_DR = sum(n),
       harv_D = sum(n * (party == "DEM")),
       harv_R = sum(n * (party == "REP")),
       .by = c(item, state, county_fips)
     ) |>
     full_join(medsl_pres |> summarise(medsl_DR = sum(n), .by = c(state, county_fips))) |>
     left_join(validation,
               by = join_by(item, county_fips),
               relationship = "one-to-one") |>
     mutate(err_D = harv_D - valid_D,
            err_R = harv_R - valid_R)
 ),

 tar_target(
   # TODO: script to paste to kuriwaki/cvr?
   counts_flag_website, {

     county_names <- countycodes|>
       mutate(state = toupper(state))

     pres_out <- pres_summary |>
       left_join(county_names, by = c("state", "county_fips")) |>
       relocate(st, state, county_name) |>
       select(-county_fips, -state)

     pres_out |>
       write_csv("share/by-county_val.csv")
   }
 ),

 tar_target(
   count_inconsist_harv, {
     harv_long |>
       count(state, dist, choice, party, item) |>
       arrange(state, dist) |>
       filter(!is.na(choice), str_detect(choice, "(UNDERVOTE|OVERVOTE|WRITE|QUALIFIED)", negate = TRUE)) |>
       collect() |>
       filter(party != "", party %in% c("DEM", "REP")) |>
       summarize(n_names = str_flatten_comma(choice), n = n(), .by = c(state, dist, party, item)) |>
       filter(n > 1) |>
   write_csv("data/tmp_harv_inconsist.csv")


   }
  ),
# TODO: delete?
   tar_target(
     count_inconsist_medsl, {
       medsl_long |>
         rename(dist = district, choice = candidate, party = party_detailed, item = office) |>
         count(state, dist, choice, party, item) |>
         arrange(state, dist) |>
         filter(!is.na(choice), str_detect(choice, "(UNDERVOTE|OVERVOTE|WRITE|QUALIFIED)", negate = TRUE)) |>
         collect() |>
         filter(party != "NA", !is.na(party), party %in% c("DEMOCRAT", "REPUBLICAN")) |>
         summarize(n_names = str_flatten_comma(choice), n = n(), .by = c(state, dist, party, item)) |>
         filter(n > 1) |>
         write_csv("data/tmp_medsl_inconsist.csv")
     }
 ),

 tar_target(
   # TODO: too much duplication?
   cong_summary,
   harv_long |>
     filter(item %in% c("US_SEN", "US_REP"), party %in% c("DEM", "REP")) |>
     group_by(item, dist, state) |>
     summarise(harv_DR = sum(n), harv_counties = paste0(unique(county), collapse = ", ")) |>
     mutate(state = state.abb[match(state, toupper(state.name))]) |>
     left_join(read_dta("data/validation-cong.dta") |>
                 filter(year == 2020, office %in% c("S", "H"), party %in% c("D", "R")) |>
                 select(office, dist, state, candidatevotes) |>
                 mutate(office = ifelse(office == "S", "US_SEN", "US_REP"),
                        dist = ifelse(office == "US_SEN", "", dist)) |>
                 group_by(office, dist, state) |>
                 summarise(valid_DR = sum(candidatevotes)),
               by = c("item" = "office", "dist" = "dist", "state" = "state")) |>
     left_join(medsl_long |>
                 filter(office %in% c("US SENATE", "US HOUSE"), party_detailed %in% c("DEMOCRAT", "REPUBLICAN")) |>
                 mutate(office = ifelse(office == "US SENATE", "US_SEN", "US_REP"),
                        district = ifelse(district == "STATEWIDE", ifelse(office == "US_REP", 1, ""),
                                          str_remove(district, "^0+")),
                        state = state.abb[match(state, toupper(state.name))]) |>
                 select(-c(county_fips, party_detailed, candidate)) |>
                 group_by(office, district, state) |>
                 summarise(medsl_DR = sum(n)),
               by = c("item" = "office", "state" = "state", "dist" = "district")) |>
     mutate(harv_cov = harv_DR/valid_DR, medsl_cov = medsl_DR/valid_DR) |>
     mutate(dist = ifelse(item == "US_REP", glue("{state}-{dist}"), glue("{state}-SEN")))
     # mutate(dist = ifelse(item == "US_REP", paste0(state, "-", dist), paste0(state, "-", "SEN")))
 ),

 tar_target(
   write_cong_summary,

   cong_summary |>
     write_csv("data/tmp_cong_summary.csv")

   # MIT Duplicate votes in: Miami-Dade, Broward, Citrus (FL)
   # MIT Diff vote values in Alaska — ask MIT to clarify? Seemingly 3 rows for each vote, plus one row with voted == 0
   # MIT voted == NA in Baca (CO)
   # Harvard Rhode Island + Candler, GA
 ),

 tar_target(
   cong_summary_website,

   cong_summary |> select(item, dist, harv_DR, valid_DR, harv_counties) |>
     write_csv("share/by-district_val.csv")
 )
)
