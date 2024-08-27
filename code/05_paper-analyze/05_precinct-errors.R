library(tidyverse)
library(arrow)
source("00_paths.R")

# counties to compare
ctys <- open_dataset(path(PATH_parq, "release")) |>
  distinct(state, county_name) |> collect()

# sum to precinct level (President, Biden + Trump) -----
prec_cvr <- open_dataset(path(PATH_parq, "intermediate/coalesced/")) |>
  semi_join(ctys, by = c("state", "county_name")) |>
  filter(office == "US PRESIDENT",
         party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |>
  count(state, county_name, precinct_medsl, party_detailed, name = "n_cvr") |>
  rename(precinct = precinct_medsl) |>
  collect()

# format returns ---
prec_v <- open_dataset(path(PATH_parq, "returns/by-precinct-mode/")) |>
  filter(office == "US PRESIDENT",
         party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |>
  collect() |>
  # Fix oddity in HUMBOLDT, CA precincts
  tidylog::mutate(precinct = if_else(
    county_name == "HUMBOLDT" & state == "CALIFORNIA",
    str_extract(precinct, "(?<=\\d{5}).+?(?=\\_)"), precinct)) |>
  # Remove individual voting modes if mode TOTAL exists for precinct
  tidylog::filter(!(any(mode == "TOTAL") & any(mode != "TOTAL")),
                  .by = c("state", "county_name",
                          "precinct", "party_detailed")) |>
  count(state, county_name, precinct, party_detailed, name = "n_v", wt = votes)

# join by "precinct"
prec_comp <- prec_cvr |>
  tidylog::left_join(
    prec_v,
    by = c("state", "county_name", "precinct", "party_detailed"),
    relationship = "one-to-one") |>
  filter(!is.na(precinct)) |>
  mutate(cand = case_match(
    party_detailed,
    "REPUBLICAN" ~ "Trump Votes", "DEMOCRAT" ~ "Biden Votes")) |>
  # redacted
  tidylog::filter(n_v > -1)

# plot comparison ----
fig_prec <- prec_comp |>
  filter(!is.na(n_v), !is.na(n_cvr)) |>
  ggplot(aes(n_v, n_cvr)) +
  geom_abline(alpha = 0.5, linetype = "dotted") +
  geom_point(size = 0.2, alpha = 0.4) +
  scale_x_continuous(trans = scales::sqrt_trans(), labels = scales::comma) +
  scale_y_continuous(trans = scales::sqrt_trans(), labels = scales::comma) +
  lemon::facet_rep_wrap(~ cand) +
  coord_equal() +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(x = "Votes for Candidate in External Precinct Return Database",
       y = "Votes in CVR"
  )

ggsave("figs/figure_03.pdf", fig_prec, w = 6, h = 3)

# stats ----
n_fmt_write <- \(x, file, dir = "numbers") {
  x |> nrow() |>
    format(big.mark = ",") |>
    write_lines(path(dir, file))
}

prec_comp |> filter(!is.na(n_v)) |> distinct(state, county_name, precinct) |>
  n_fmt_write("N_precs-val_all.tex")

prec_comp |> filter(!is.na(n_v)) |> distinct(state, county_name) |>
  n_fmt_write("N_counties_precs-val.tex")

prec_comp |> filter(!is.na(n_v)) |>
  filter(n_cvr == n_v) |> distinct(state, county_name, precinct) |>
  n_fmt_write("N_precs-val_perfect.tex")

prec_comp |> filter(!is.na(n_v)) |>
  filter(between(n_cvr, n_v - 3, n_v + 3)) |> distinct(state, county_name, precinct) |>
  n_fmt_write("N_precs-val_within3.tex")
