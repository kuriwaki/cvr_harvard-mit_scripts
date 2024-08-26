library(tidyverse)
library(sf)
library(scales)
library(arrow)
# library(ggsflabel) # pak::pkg_install("yutannihilation/ggsflabel")

shp_us <- urbnmapr::get_urbn_map(map = "states", sf = TRUE) # pak::pkg_install("UrbanInstitute/urbnmapr")

ds <- open_dataset("release")
dsa_v <- open_dataset("returns/by-county/")

# Count state level
st_total_votes <- dsa_v |> 
  count(state, office, wt = votes) |> 
  collect() |> 
  mutate(fac = ifelse(state == "ARIZONA" & office == "STATE HOUSE", 0.5, 1)) |> 
  transmute(state, office, total_voters = n*fac)

st_counts <- ds |> 
  filter(!state %in% c("VIRGINIA", "IOWA")) |> 
  count(state, office) |> 
  collect() |> 
  mutate(fac = ifelse(state == "ARIZONA" & office == "STATE HOUSE", 0.5, 1)) |> 
  mutate(n = n*fac, fac = NULL) |> 
  right_join(st_total_votes) |> 
  mutate(frac_covered = n/total_voters) # |> 
  # tidylog::mutate(frac_covered = replace(frac_covered, n > total_voters, 1))
  

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

# counts
gg_st <- plot_dat |> 
  ggplot(aes(fill = n)) +
  geom_sf(lwd = 0.1) +
  facet_wrap(~ office) +
  scale_fill_fermenter(
    breaks = c(1e3, 1e4, 1e5, 1e6, 2e6, 4e6, 8e6), na.value = "white", 
    palette = "Oranges",
    labels = scales::comma,
    direction = 1) +
  theme_void() +
  theme(strip.text = ggtext::element_markdown()) +
  labs(fill = "Voters in\nCVR Data")

# fractions
gg_cov <-
  gg_st +
  aes(fill = frac_covered) +
  geom_sf_text(
    aes(label = scales::percent(frac_covered, accuracy = 1),
        color = frac_covered < 0.75),
    size = 1.6) +
  scale_color_manual(values = c(`TRUE` = "navy", `FALSE` = "white")) +
  scale_fill_fermenter(
    palette = "Greys",
    breaks = c(0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
    na.value = "white",
    label = scales::percent,
    direction = 1) +
  guides(color = "none") +
  labs(fill = "CVR Coverage")

ggsave("paper/figs/coverage_frac.png",
       gg_cov, dpi = 400,
       w = 7.5, h = 4)
  
