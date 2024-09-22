suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(sf)
  library(patchwork)
})

source("00_paths.R")

# Make state names to postal abbreviations for merge
state2abb <- state.abb
names(state2abb) <- stringr::str_to_upper(state.name)

# Get counties in CVR collection
cvr_county <- arrow::open_dataset(str_c(PATH_parq, "/release")) |>
  filter(
    office == "US PRESIDENT",
    candidate %in% c("DONALD J TRUMP", "JOSEPH R BIDEN")
  ) |>
  count(state, county_name, name = "cvr_votes") |>
  collect() |>
  mutate(state_po = state2abb[state])

# Merge with MEDSL counties from precinct data collections.
#  - Helps with merge to census county shapes
#  - Allows double check of vote counts in CVR data (can check Trump + Biden count)
all_county <- arrow::open_dataset(PATH_medsl_precincts,
  unify_schemas = FALSE,
  schema = schema(
    field("county_name", string()),
    field("county_fips", string()),
    field("state_po", string()),
    field("precinct", string()),
    field("office", string()),
    field("district", string()),
    field("party_simplified", string()),
    field("mode", string()),
    field("votes", double())
  )
) |>
  to_duckdb() |>
  filter(
    office == "US PRESIDENT",
    party_simplified %in% c("DEMOCRAT", "REPUBLICAN")
  ) |>
  collect() |>
  filter(!any(mode == "TOTAL") | mode == "TOTAL",
    .by = c(state_po, county_name)
  ) |>
  summarize(
    prec_votes = sum(votes),
    modes = list(mode),
    .by = c(
      state_po,
      county_name,
      county_fips
    )
  ) |>
  mutate(
    modes = map_chr(modes, \(x) str_flatten(sort(unique(x)),
      collapse = ";"
    )),
    county_fips = str_pad(county_fips,
      width = 5, side = "left",
      pad = "0"
    )
  ) |>
  left_join(cvr_county,
    by = c("state_po", "county_name")
  ) |>
  arrange(state_po, county_name) |>
  select(
    state, state_po, county_name, modes,
    county_fips, cvr_votes, prec_votes
  )

## Get county shapes and merge voting data
county_shp <- tigris::counties(class = "sf", cb = TRUE) |>
  mutate(county_fips = paste(county_fips = str_trim(paste0(STATEFP, COUNTYFP)))) |>
  full_join(all_county, by = "county_fips")

## Get state outlines to make map prettier
state_shp <- tigris::states(class = "sf", cb = TRUE)

## Make nationwide map of counties with CVRs in our released collection
gg_county <- county_shp |>
  filter(STATE_NAME %in% state.name) |>
  mutate(
    covered = case_when(
      !is.na(cvr_votes) ~ "c",
      STATE_NAME %in% c(
        "Delaware",
        "Rhode Island"
      ) ~ "c",
      any(!is.na(cvr_votes)) ~ "b",
      TRUE ~ "a"
    ),
    .by = STATE_NAME
  ) |>
  tigris::shift_geometry() |>
  ggplot() +
  geom_sf(aes(fill = covered),
    color = "white"
  ) +
  geom_sf(
    data = state_shp |>
      filter(NAME %in% state.name) |>
      tigris::shift_geometry(),
    col = "white",
    linewidth = 0.6,
    fill = NA
  ) +
  scale_fill_manual(
    values = c(
      "a" = "#F0F0F0",
      "b" = "lightgrey",
      "c" = "black"
    ),
    guide = "none"
  ) +
  ggthemes::theme_map()


# ggsave("figs/figure_county.pdf",
#   gg_county,
#   w = 10
# )


## State grid of maps of counties with CVRs in our released collection
state_map <- function(the_state) {
  county_shp |>
    filter(str_to_upper(STATE_NAME) == the_state) |>
    mutate(
      covered = case_when(
        !is.na(cvr_votes) ~ TRUE,
        STATE_NAME %in% c(
          "Rhode Island",
          "Delaware"
        ) ~ TRUE,
        TRUE ~ FALSE
      ),
      covered = factor(covered, levels = c(TRUE, FALSE))
    ) |>
    st_transform("ESRI:102003") |>
    ggplot() +
    geom_sf(aes(fill = covered),
      color = "white"
    ) +
    scale_fill_manual(
      values = c(
        "FALSE" = "lightgrey",
        "TRUE" = "black"
      ),
      guide = "none"
    ) +
    ggthemes::theme_map() +
    theme(strip.text = ggtext::element_markdown())
}

cvr_states <- sort(unique(cvr_county$state))
mps <- map(cvr_states, state_map)
gg_county_grid <- cowplot::plot_grid(
  plotlist = mps,
  labels = str_to_title(cvr_states),
  align = "n",
  label_size = 10,
  hjust = 0.5,
  vjust = 1.0,
  label_x = 0.5,
  label_fontface = "plain",
  greedy = FALSE,
  scale = 0.90
)

# ggsave("figs/figure_county_grid.pdf",
#   gg_county_grid,
#   w = 10
# )

gg_county / gg_county_grid +
  theme(plot.tag = element_text(size = 12, face = "plain")) +
  plot_annotation(tag_levels = "a")

ggsave("figs/figure_county_combined.pdf", w = 10, units = "in")
