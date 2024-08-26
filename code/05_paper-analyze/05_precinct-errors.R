library(tidyverse)
library(arrow)



# count to precinct level
prec_cvr <- open_dataset("release") |> 
  filter(office == "US PRESIDENT",
         party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |> 
  count(state, county_name, precinct_medsl, party_detailed, name = "n_cvr") |> 
  rename(precinct = precinct_medsl) |> 
  collect()

prec_v <- open_dataset("returns/by-precinct-mode/") |> 
  filter(office == "US PRESIDENT",
         party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |>
  collect() |>
  # Fix oddity in HUMBOLDT, CA precincts (JBL)
  mutate(precinct = if_else(county_name == "HUMBOLDT" & 
                            state == "CALIFORNIA",
                            str_extract(precinct, 
                                        "(?<=\\d{5}).+?(?=\\_)"),
                            precinct)) |>
  # Remove individual voting modes if mode TOTAL exists for 
  # precinct (JBL)
  filter(!(any(mode == "TOTAL") & any(mode != "TOTAL")),
         .by = c("state", "county_name", 
                 "precinct", "party_detailed")) |>  
  count(state, county_name, precinct, party_detailed, name = "n_v", wt = votes) 

# join
prec_comp <- prec_cvr |> 
  tidylog::left_join(
    prec_v,
    by = c("state", "county_name", "precinct", "party_detailed")) |> 
  filter(!is.na(precinct)) |> 
  mutate(cand = case_match(
    party_detailed, 
    "REPUBLICAN" ~ "Trump Votes", "DEMOCRAT" ~ "Biden Votes")) |> 
  # redacted
  filter(n_v > -1)

# check
prec_comp |> 
  filter(n_v > n_cvr + 100) |> 
  count(state, county_name, sort = TRUE)

# plot
prec_comp |> 
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
  labs(x = "Votes for Candidate in Precinct Return Database (Baltz et al.)",
       y = "Votes in CVR (matched precinct-level)"
  )

ggsave("paper/figs/precinct_discrep.png", w = 6, h = 3)

prec_comp |> filter(!is.na(n_v)) |> distinct(state, county_name, precinct) |> 
  nrow() |> format(big.mark = ",") |>  
  write_lines("paper/numbers/N_precs_precs-val.tex")

prec_comp |> filter(!is.na(n_v)) |> distinct(state, county_name) |> 
  nrow() |> format(big.mark = ",") |> 
  write_lines("paper/numbers/N_counties_precs-val.tex")



# sketch
prec_comp |> filter(!is.na(n_v)) |> 
  filter(n_cvr == n_v) |>  distinct(state, county_name, precinct) |> 
  nrow() |> format(big.mark = ",") |> 
  write_lines("paper/numbers/N_precs-val_perfect.tex")

prec_comp |> filter(!is.na(n_v)) |>  
  filter(between(n_cvr, n_v - 3, n_v + 3)) |> distinct(state, county_name, precinct) |> 
  nrow() |> format(big.mark = ",") |> 
  write_lines("paper/numbers/N_precs-val_within3.tex")

prec_comp |> filter(!is.na(n_v)) |> distinct(state, county_name, precinct) |> 
  nrow() |> format(big.mark = ",") |> 
  write_lines("paper/numbers/N_precs-val_all.tex")


prec <- open_dataset("release") |> 
  filter(office == "US PRESIDENT",
         party_detailed %in% c("REPUBLICAN", "DEMOCRAT")) |> 
  count(state, county_name, precinct, precinct_medsl)

prec |> 
  collect()

prec |> 
  collect() |> 
  filter(is.na(precinct_medsl)) |> 
  count(state, county_name)
