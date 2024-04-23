library(arrow)
library(tidyverse)
library(ccesMRPprep)
# source("R/sum_cand-counts.R")

# Harvard data ----
cd_val <- read_csv("~/Dropbox/CVR_Harvard-MIT/data/val/ush2020_by-cd.csv")
db <- open_dataset("~/Dropbox/CVR_Harvard-MIT/data/harvard/cvrs_harv_medsl-fmt")


stb_dat <- read_dta("tmp_state_ballot_measures_merged.dta") |> 
  summarize(N = max(N), .by = state)

st_state <- transmute(states_key, state = str_to_upper(state), st)

db_count <- db |>
  filter(office == "US HOUSE") |>
  count(state, district, party_detailed) |>
  collect() |>
  left_join(st_state, by = "state") |>
  filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN")) |>
  mutate(dist = recode(district, STATEWIDE = "001"),
         dist = str_extract(dist, "[0-9]+")) |>
  mutate(cd = to_cd(st, dist)) |>
  count(cd, wt = n, name = "N_DR")

# Combine Harvard Data ---
cov_stat <- cd_val |>
  left_join(db_count) |>
  mutate(N_DR = replace_na(N_DR, 0),
         N_DR = pmin(N_DR, N_all)
  ) |>
  # manual
  mutate(N_DR = if_else(str_detect(cd, "(AK|DE|RI)"), N_all, N_DR)) |> 
  mutate(cov = N_DR/N_all) |> 
  tidylog::mutate(cov = na_if(cov, 0))

dat_use <- donnermap::cd_shp |>
  left_join(cov_stat, by = "cd")

# State Ballot
st_val <- cd_val |> 
  mutate(state = str_sub(cd, 1, 2)) |> 
  count(state, wt = N_all, name = "N_all")

cov_state <- stb_dat |> 
  left_join(st_val) |> 
  mutate(cov = N / N_all)

dat_use_st <- donnermap::st_shp |> 
  rename(state = STATEAB) |> 
  left_join(cov_state)



# Plot ----
gg0 <- dat_use |>
  ggplot(aes(fill = cov)) +
  geom_sf() +
  scale_fill_fermenter(
    "Collected",
    direction = 1,
    breaks = c(0.05, 0.10, 0.5, 0.9, 0.95),
    labels = scales::percent,
    na.value = "white") +
  theme_void() +
  theme(legend.position = c(0.9, 0.25),
        plot.title = element_text(face = "bold", hjust = 0.5))


gg1 <- gg0 %+% dat_use_st

gg_cd <- gg0 +
  labs(title = "U.S. Presidential Votes")
gg_stb <- gg1 + 
  labs(title = "Statewide Ballot Measures") + 
  guides(fill = "none")

gg_lb <- make_map()
gg_spend <- make_map(tit = "Spending-Related", col = "darkolivegreen3", spend = TRUE)
# gg_ed <- make_map(c("Education", "Fire"), "Education and Fire Spending", col = "darkolivegreen3")
# gg_wt <- make_map(c("Water", "Roads", "Genl_Services"), "Road, Sewage, General Spending", col = "orange")

gg_cd + gg_stb + gg_lb + gg_spend +
  plot_layout(nrow = 2)

ggsave(fs::path(paperdir, "figures/localballot_map.pdf"), 
       w = 9, h = 6)

