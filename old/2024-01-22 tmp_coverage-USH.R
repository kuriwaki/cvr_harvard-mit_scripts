library(arrow)
library(tidyverse)
library(ccesMRPprep)
source("R/sum_cand-counts.R")

# Harvard data ----
cd_val <- read_csv("data/val/ush2020_by-cd.csv")
harv_counts <- read_csv("data/tmp_ushou_harv.csv")
ca_add <- read_csv("data/harvard/tmp_losangeles-orange_USH.csv")


# MIT -----
medsl_db <- open_dataset("data/MEDSL/cvrs_statewide/")

st_state <- transmute(states_key, state = str_to_upper(state), st)
medsl_count <- medsl_db |>
  filter(office == "US HOUSE") |>
  count(state, district, party_detailed) |>
  collect() |>
  left_join(st_state, by = "state")

ca_add_tbl <- ca_add |>
  filter(!candidate %in% c("No Selection", "Undervote")) |>
  count(dist, wt = n, name = "N_DR") |>
  mutate(state = "CA") |>
  transmute(cd = to_cd(state, dist),
            N_DR)

medsl_add_tbl <- medsl_count |>
  filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN")) |>
  mutate(dist = recode(district, STATEWIDE = "001"),
         dist = str_extract(dist, "[0-9]+")) |>
  mutate(cd = to_cd(st, dist)) |>
  count(cd, wt = n, name = "N_MEDSL")

# Combine Harvard Data ---
harv_new <- harv_counts |>
  select(cd, N_DR = N_harv) |>
  bind_rows(ca_add_tbl) |>
  count(cd, wt = N_DR, name = "N_DR")

# compute frac
cov_stat <- cd_val |>
  left_join(harv_new) |>
  left_join(medsl_add_tbl) |>
  mutate(N_DR = replace_na(N_DR, 0),
         N_MEDSL = replace_na(N_MEDSL, 0),
         N_DR = pmin(N_DR, N_all),
         N_MEDSL = pmin(N_MEDSL, N_all),
         ) |>
  mutate(cov = N_DR/N_all,
         cov_MEDSL = pmax(N_DR, N_MEDSL) / N_all) |>
  tidylog::mutate(cov = na_if(cov, 0), cov_MEDSL = na_if(cov_MEDSL, 0))

dat_use <- donnermap::cd_shp |>
  left_join(cov_stat, by = "cd")


# Plot ----
gg0 <- dat_use |>
  ggplot(aes(fill = cov_MEDSL)) +
  geom_sf() +
  scale_fill_fermenter(
    "Collected",
    direction = 1,
    breaks = c(0.05, 0.10, 0.5, 0.9, 0.95),
    labels = scales::percent,
    na.value = "white") +
  theme_void() +
  theme(legend.position = c(0.9, 0.25))

gg0 + aes(fill = cov)
ggsave("reports/tmp_coverage_harvard-only.pdf", w = 5, h = 3)
gg0
ggsave("reports/tmp_coverage_harvard-MEDSL.pdf", w = 5, h = 3)


# stats
sum(dat_use$cov_MEDSL*dat_use$N_all, na.rm = TRUE) / sum(dat_use$N_all)
filter(dat_use, cov_MEDSL > 0) |> nrow()
