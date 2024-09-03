suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(sf)
  library(scales)
  library(urbnmapr) # renv::install("UrbanInstitute/urbnmapr")
})
source("00_paths.R")

shp_us <- urbnmapr::get_urbn_map(map = "states", sf = TRUE) #

ds <- open_dataset(path(PATH_parq, "release"))
dsa_v <- open_dataset(path(PATH_parq, "returns/by-county/"))

# Count state level
st_total_votes <- dsa_v |>
  filter(!party_simplified %in% c("UNDERVOTE", "OVERVOTE")) |>
  count(state, office, wt = votes) |>
  collect() |>
  mutate(fac = ifelse(state == "ARIZONA" & office == "STATE HOUSE", 0.5, 1)) |>
  transmute(state, office, total_voters = n*fac)

st_counts <- ds |>
  filter(!state %in% c("VIRGINIA")) |>
  filter(!candidate %in% c("UNDERVOTE", "OVERVOTE")) |>
  count(state, office) |>
  collect() |>
  mutate(fac = ifelse(state == "ARIZONA" & office == "STATE HOUSE", 0.5, 1)) |>
  mutate(n = n*fac, fac = NULL) |>
  right_join(st_total_votes) |>
  mutate(frac_covered = n/total_voters) |>
# so it doesn't round to 0 in the figure
  tidylog::mutate(frac_covered = ifelse(frac_covered > 0 & frac_covered < 0.005,
                                        0.01, frac_covered))


# Label formatting
plot_dat <- shp_us |>
  mutate(state = str_to_upper(state_name)) |>
  left_join(st_counts, by = "state") |>
  # complete(nesting(state, geometry), office, fill = list(n = NA_real_)) |>
  mutate(office = factor(
    office,
    labels = c("US President", "US Senate", "US House",
               "Governor", "State Senate", "State House"),
    levels = c("US PRESIDENT", "US SENATE", "US HOUSE",
               "GOVERNOR", "STATE SENATE", "STATE HOUSE"))) |>
  filter(!is.na(office)) |>
  st_sf()

# fractions
gg_cov <-
  plot_dat |>
  ggplot(aes(fill = n)) +
  facet_wrap(~ office) +
  geom_sf(lwd = 0.1) +
  aes(fill = frac_covered) +
  geom_sf_text(
    aes(label = scales::percent(frac_covered, accuracy = 1),
        color = frac_covered < 0.75),
    size = 1.6) +
  scale_color_manual(values = c(`TRUE` = "navy", `FALSE` = "white")) +
  scale_fill_fermenter(
    palette = "Greys",
    breaks = c(0.25, 0.5, 0.75, 0.9, 0.95),
    na.value = "white",
    label = scales::percent,
    direction = 1) +
  guides(color = "none") +
  theme_void() +
  theme(strip.text = ggtext::element_markdown()) +
  labs(fill = "CVR Coverage")

ggsave("figs/figure_S1.pdf",
       gg_cov,
       w = 7.5, h = 4)

