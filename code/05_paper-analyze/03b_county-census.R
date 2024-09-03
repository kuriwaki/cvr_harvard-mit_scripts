library(tidyverse)
library(tidycensus)
library(arrow)
library(gt)
library(fs)
source("00_paths.R")

calc_summary_stats <- function(data, variable) { # func to calculate sum stats
  tibble(
    Variable = variable,
    Mean = mean(data[[variable]], na.rm = TRUE),
    Median = median(data[[variable]], na.rm = TRUE),
    SD = sd(data[[variable]], na.rm = TRUE)
  )
}

calc_se <- function(x, y) { # calculate standard errors for difference in means
  sqrt(var(x, na.rm = TRUE) / length(na.omit(x)) +
    var(y, na.rm = TRUE) / length(na.omit(y)))
}

# set API (include your own)
# census_api_key("XXXXXX", install = TRUE, overwrite = TRUE)


# load var names
dec2020dhc_vars <- load_variables(
  year = 2020,
  "dhc",
  cache = TRUE
)

# store vars needed - dhc dataset
vars <- c(
  "P1_001N", # total population
  "P2_001N", # total urban and rural
  "P2_002N", # total urban
  "H4_001N", # total home ownership
  "H4_002N", # total home own mortgage
  "H4_003N", # total home own free
  "P3_001N", # total races
  "P3_002N", # total White
  "P3_003N", # total Black
  "P4_001N", # total Hispanic or latin Origin
  "P4_003N", # total Hispanic or Latin
  "P5_001N", # total Hispanic by Race
  "P5_010N", # total Hispanic or Latin
  "P12_001N", # total age
  "P12_002N", # total male
  "P12_003N", # total male under 5
  "P12_004N",
  "P12_005N",
  "P12_006N", # total male 16-17
  "P12_020N", # total male 65+
  "P12_021N",
  "P12_022N",
  "P12_023N",
  "P12_024N",
  "P12_025N",
  "P12_026N", # total female
  "P12_027N", # total female under 18
  "P12_028N",
  "P12_029N",
  "P12_030N",
  "P12_044N", # total female 65+
  "P12_045N",
  "P12_046N",
  "P12_047N",
  "P12_048N",
  "P12_049N"
)



## load census data using get_decennial function from tidycensus
cen <- get_decennial(
  year = 2020,
  geography = "county",
  keep_geo_vars = TRUE,
  variables = vars,
  sumfile = "dhc",
  output = "wide"
) |>
  dplyr::rename(
    tot = P1_001N,
    tot_urb_rur = P2_001N,
    tot_urb = P2_002N,
    tot_own = H4_001N,
    tot_own_mort = H4_002N,
    tot_own_free = H4_003N,
    tot_age = P12_001N,
    tot_races = P3_001N,
    white = P3_002N,
    black = P3_003N,
    hisp_og_tot = P4_001N,
    hisp_og = P4_003N,
    hisp_race_tot = P5_001N,
    his_race = P5_010N
  ) |>
  mutate(
    prop_urb = tot_urb / tot_urb_rur * 100, # calculate proportion vars
    prop_white = white / tot_races * 100,
    prop_black = black / tot_races * 100,
    prop_hisp = hisp_og / hisp_og_tot * 100,
    prop_hisp_race = his_race / hisp_race_tot * 100,
    own_occupy = rowSums(across(c(tot_own_mort, tot_own_free)),
      na.rm = TRUE
    ),
    prop_own_occ = own_occupy / tot * 100,
    all18 = rowSums(
      across(c(
        P12_003N,
        P12_004N,
        P12_005N,
        P12_006N,
        P12_027N, P12_028N,
        P12_029N, P12_030N
      )),
      na.rm = TRUE
    ),
    all65 = rowSums(
      across(c(
        P12_020N, P12_021N,
        P12_022N, P12_023N,
        P12_024N, P12_025N,
        P12_044N, P12_045N,
        P12_046N, P12_047N,
        P12_048N, P12_049N
      )),
      na.rm = TRUE
    ),
    prop_u18 = all18 / tot_age * 100,
    prop_o65 = all65 / tot_age * 100,
    county = str_trim(str_extract(
      NAME,
      ".*(?= County)"
    )), # extract everything before "County"
    state = str_trim(str_extract(
      NAME,
      "(?<=, ).*"
    )), # extract everything after common and space
    county = toupper(county)
  ) |>
  select(-starts_with("P1"))

sum_vars <- c(
  "prop_urb", "prop_white", # designate our final summary variables
  "prop_black", "prop_hisp",
  "prop_u18", "prop_o65",
  "prop_own_occ"
)

## calc summary stats for all counties
summary_all <- bind_rows(
  lapply(sum_vars, function(var) calc_summary_stats(cen, var))
)





## load cvr data subset
cty_codes <- tidycensus::fips_codes |>
  mutate(
    county_fips = str_c(state_code, county_code) |>
      str_pad(pad = "0", width = 5),
    state_name = str_to_upper(state_name),
    county = str_to_upper(str_remove_all(county, " County$")),
    county = str_replace(county, "^ST\\.", "ST")
  ) |>
  select(-c(state, state_code, county_code)) |>
  rename(state = state_name, county_name = county) |>
  as_tibble()

ds <- open_dataset(path(PATH_parq, "release")) |>
  select(county_name, state) |>
  distinct() |>
  collect() |>
  tidylog::inner_join(cty_codes, by = c("state", "county_name")) |>
  bind_rows(
    tribble(
      ~state, ~county_name, ~county_fips,
      "DELAWARE", "KENT", "10001",
      "DELAWARE", "NEW CASTLE", "10003",
      "DELAWARE", "SUSSEX", "10005",
      "RHODE ISLAND", "BRISTOL", "44001",
      "RHODE ISLAND", "KENT", "44003",
      "RHODE ISLAND", "NEWPORT", "44005",
      "RHODE ISLAND", "PROVIDENCE", "44007",
      "RHODE ISLAND", "WASHINGTON", "44009",
    )
  ) |>
  filter(county_name != "STATEWIDE")

cvr_cen <- ds |>
  select(-state) |>
  tidylog::left_join(cen, by = c("county_fips" = "GEOID"))


## calc summary stats for cvr data counties
summary_subset <- bind_rows(
  lapply(
    sum_vars,
    function(var) {
      calc_summary_stats(
        cvr_cen,
        var
      )
    }
  )
)


## combine census and cvr subset
sum_stats <- data.frame(
  Statistic = rep(c("Mean", "Median", "SD"), each = 7),
  Variable = rep(c(
    "Percent Urban", "Percent White",
    "Percent Black", "Percent Hispanic",
    "Percent Under 18", "Percent Over 65",
    "Percent Homeowning"
  ), 3),
  `All U.S. Counties` = c(
    summary_all$Mean,
    summary_all$Median,
    summary_all$SD
  ),
  `Our Data` = c(
    summary_subset$Mean,
    summary_subset$Median,
    summary_subset$SD
  )
)


## Create Table D.1
sumstats_w <- sum_stats |>
  pivot_wider(
    names_from = Statistic,
    values_from = c(
      All.U.S..Counties,
      Our.Data
    )
  ) |>
  dplyr::select(
    Variable,
    `Mean_OurData` = Our.Data_Mean,
    `Mean_Census` = All.U.S..Counties_Mean,
    `Median_OurData` = Our.Data_Median,
    `Median_Census` = All.U.S..Counties_Median,
    `SD_OurData` = Our.Data_SD,
    `SD_Census` = All.U.S..Counties_SD
  ) |>
  relocate(Variable, starts_with("Mean"), starts_with("Median"), starts_with("SD")) |>
  gt() |>
  tab_options(table.font.size = px(13)) |>
  fmt_number(matches("_Census|_OurData"), decimals = 1) |>
  cols_label_with(fn = \(x) case_when(x == "Variable" ~ "", str_detect(x, "OurData") ~ "CVR", str_detect(x, "Census") ~ "Nation")) |>
  tab_spanner("Mean", columns = matches("^Mean")) |>
  tab_spanner("Median", columns = matches("^Median")) |>
  tab_spanner("Standard Dev.", columns = matches("^SD"))


sumstats_w |>
  gtsave("tables/table_03.tex")
