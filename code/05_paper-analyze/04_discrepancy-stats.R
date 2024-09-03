suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(glue)
  library(readxl)
  library(patchwork)
  library(gt)
})
source("00_paths.R")

# Read in comparison data
dat_cand <- read_excel(path(PATH_parq, "combined/compare.xlsx"),
                       sheet = "by-cand-coalesced") |>
  # for this analysiss, filter to candidates
  filter(!(party_detailed %in% c("UNDERVOTE","WRITEIN"))) |>
  filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN"))

dat_county <- read_excel(path(PATH_parq, "combined/compare.xlsx"), sheet = 2)

used_val <- c(`0` = "gray", `1` = "black")
used_lab <- c(`0` = "Not Released", `1` = "Released")

#### Base tables ####
county_level <- dat_county |>
  filter(state != "DISTRICT OF COLUMBIA",
         county_name != "ARAPAHOE") |>
  mutate(color2_c = recode(color2_c, red = "any > 10% mismatch",
                           `no entry in Baltz`= "candidate missing")) |>
  count(color2_c, release) |>
  mutate(color2_c = factor(
    color2_c,
    levels = c(
      "0 difference",
      "any < 1% mismatch",
      "any < 5% mismatch",
      "any < 10% mismatch",
      "any > 10% mismatch",
      "candidate missing",
      # "no entry in Baltz",
      "unclassified"),
    labels = c(
      "0 difference" = "0 discrepancy",
      "any < 1% mismatch" = "any <1% discrepancy",
      "any < 5% mismatch" = "any < 5% discrepancy",
      "any < 10% mismatch" = "any < 10% discrepancy",
      "any > 10% mismatch" = "any > 10% discrepancy",
      "candidate missing",
      # "no entry in Baltz",
      "unclassified"
    )
  )
  )

# State x Office-level % differences
cand_dist_level <- dat_cand |>
  group_by(state, office, county_name, district, release) |>
  summarise(
    cvr_votes = sum(votes_c, na.rm = TRUE),
    total_votes = sum(votes_v, na.rm = TRUE),
    diff = cvr_votes - total_votes,
    # Calculate % difference only if we have votes in both CVR and precinct
    perc_diff = ifelse(total_votes != 0 & cvr_votes != 0, diff/total_votes, NA),
    .groups = "drop"
  ) |>
  tidylog::filter(total_votes >= 150) # |>  # avoid super big percentages


#### Figures ####
## county level
gg_gt <- county_level |>
  arrange(color2_c) |>
  gt::gt() |>
  gt::cols_label("color2_c" ~ "",
                 release ~ "Released",
                 n ~ "N") |>
  as_gtable()

##### State x Office #####
fig_b <- cand_dist_level |>
  filter(!(state == "RHODE ISLAND" & district == "057" & office == "STATE HOUSE")) |>
  ggplot(aes(x = total_votes, y = perc_diff, color = factor(release))) +
  geom_point(alpha = 0.4) +
  scale_y_continuous(labels = scales::percent) +
  scale_x_log10(labels = scales::comma) +
  scale_color_manual(values = used_val, labels = used_lab) +
  theme_classic() +
  labs(fill = NULL) +
  theme(legend.position = "none", legend.position.inside = c(0.8, 0.8)) +
  labs(x = "Total Votes Cast", y = "Discrepancy (%)", color = NULL)

### Histogram
fig_c <- cand_dist_level |>
  ggplot(aes(x = perc_diff,
             fill = factor(release))) +
  geom_histogram(
    aes(y = after_stat(count/sum(count))),
    position = position_stack(),
    boundary = 0,
    color = "white",
    linewidth = 0.1,
    binwidth = 0.002) +
  theme_classic() +
  scale_fill_manual(values = used_val, labels = used_lab) +
  scale_y_continuous(labels = scales::percent,
                     breaks = seq(0, 0.8, 0.2),
                     limits = c(0, 0.8)) +
  scale_x_continuous(labels = \(x) scales::percent(x, accuracy = 0.1, suffix = ""),
                     breaks = c(-0.05, -0.03, -0.01, 0, 0.01),
                     expand = expansion(add = c(0, 0.0001))) +
  coord_cartesian(xlim = c(-0.05, 0.01)) +
  labs(fill = NULL, x = "Discrepancy (%)", y = "Fraction of sample",
       caption = "x-axis display limited to [-0.05, +0.01]") +
  theme(legend.position = "inside", legend.position.inside = c(0.4, 0.9))


wrap_elements(gg_gt) + fig_b + fig_c +
  plot_layout(widths = c(1, 0.5, 0.5)) +
  plot_annotation(tag_levels = "a")
ggsave("figs/figure_02.pdf", w = 10, h = 3, units = "in")



# Stats -----
dat_county |>
  filter(color2_c == "0 difference") |>
  nrow() |>
  write_lines("numbers/counties_0-disc.tex")

dat_county |>
  filter(color2_c == "any < 1% mismatch") |>
  nrow() |>
  write_lines("numbers/counties_1perc-disc.tex")


