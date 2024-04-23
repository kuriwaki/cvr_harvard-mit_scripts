library(haven)
library(arrow)
library(tidyverse)
library(gt)
library(sf)
library(gtExtras)
library(scales)
library(urbnmapr)
library(patchwork)
library(glue)


#' Count and report 
count_n <- function(tbl) {
  tbl |> 
    count() |> 
    collect() |> 
    pull(n) |> 
    scales::comma()
}

paperdir <- "~/Dropbox/CVR_papers/all-politics-national"

#' Metadata
item_info <- read_dta("../data_main/item_choice_info.dta")
item_counties <- distinct(item_info, state, county, place, item,
                          nonpartisan, bond_tax, description)

lb_summ <- read_dta("local_bm_topics.dta")

# county-level
source("~/Dropbox/CVR_Harvard-MIT/R/std_county_names.R")

county_shp <- get_urbn_map("counties", sf = TRUE) |> 
  mutate(county_name = str_to_upper(str_trim(str_remove(county_name, " County")))) |> 
  left_join(countydata) |> 
  relocate(state = state_abbv, county_name)


lb_clvl <- lb_summ |> 
  summarize(
    N_voters = mean(N),
    .by = c(state, topic, place, item)) |> 
  tidylog::left_join(item_counties, relationship = "many-to-many") |> 
  tidylog::mutate(county_name = std_county_name(county)) |> 
  left_join(select(st_drop_geometry(county_shp), state, county_name, hhpop)) |> 
  arrange(state, item, desc(hhpop)) |> 
  distinct(state, place, item, .keep_all = TRUE)


  
# Size circle by pop size, add number for number of bills
st_shp <- get_urbn_map("states", sf = TRUE)

make_map <- function(subset = NULL,  tit = "All Local Ballot Measures", col = "dodgerblue", 
                     spend = FALSE,
                     data = lb_clvl) {
  
  if (!is.null(subset) & isFALSE(spend)) {
    data <- filter(data, topic %in% subset)
  }
  
  if (isTRUE(spend)) {
    data <- filter(data, bond_tax != "")
  }
  
  data_agg <- data |> 
    count(state, county_name, hhpop)
  
  use_shp <- county_shp |> 
    mutate(centr = sf::st_centroid(geometry)) |> 
    left_join(data_agg, by = c("state", "county_name", "hhpop")) |> 
    filter(!is.na(county_name)) |> 
    mutate(county_size = replace(hhpop, is.na(n), NA)) |> 
    st_simplify()
  
  use_shp |> 
    ggplot() +
    geom_sf(data = st_shp, fill = "transparent") +
    geom_sf(aes(geometry = centr, size = county_size), 
            color =  col,
            alpha = 0.2) + 
    scale_size_area(max_size = 20) +
    geom_sf_text(
      aes(geometry = centr, label = n), 
      position = "jitter",
      size = 2, alpha = 0.8, color = "black") +
    labs(title = glue("{tit}\n(n = {scales::comma(sum(use_shp$n, na.rm = TRUE))})")) +
    theme_void() +
    guides(size = "none") +
    theme(plot.title = element_text(face = "bold", hjust = 0.5))
}



# sumamry stats ----
library(fs)
lb_clvl |> distinct(state) |> count_n() |> write_lines(path(paperdir, "n/lb_states.tex"))
lb_clvl |> distinct(state, county_name) |> count_n() |> write_lines(path(paperdir, "n/lb_counties.tex"))
lb_clvl |> distinct(state, place) |> count_n() |> write_lines(path(paperdir, "n/lb_localities.tex"))
lb_summ |> count_n() |> write_lines(path(paperdir, "n/lb_total-measures.tex"))
lb_summ |> filter(spending == 1) |> count_n() |> write_lines(path(paperdir, "n/lb_spending.tex"))
lb_summ |> filter(spending == 1) |> count(topic, sort = TRUE)

median(lb_summ$N) |> scales::comma() |> write_lines(path(paperdir, "n/lb_pop-median.tex"))
quantile(lb_summ$N, c(0.10, 0.9)) |> 
  round() |> 
  str_c(collapse = ", ") |> 
  write_lines(path(paperdir, "n/lb_pop-range.tex"))
lb_clvl |> arrange(desc(N_voters)) |> select(state, county, item, description, N_voters)

# summary table
info_measure <- item_info |> 
  filter(level == "", measure == 1)

item_measure <- info_measure |> 
  summarize(
    counties = str_flatten_comma(unique(county)),
    .by = c(state, item, description, yes_orientation, 
            place,
            spending, bond_tax, inc_dec_cont)
  ) |> 
  mutate(across(where(is.character), \(x) na_if(x, ""))) |> 
  relocate(state, place, counties) |> 
  tidylog::distinct(state, item, place, .keep_all = TRUE)



lb_summ |> 
  filter(yes_is_lib == 1, spending == 1, topic != "") |> 
  tidylog::left_join(item_measure, by = c("state", "place", "item", "spending")) |> 
  transmute(
    state, topic, place, counties,  description,
    pct_yes_R = Y_choice_R_pid / (Y_choice_R_pid + N_choice_R_pid),
    pct_yes_D = Y_choice_D_pid / (Y_choice_D_pid + N_choice_D_pid)) |> 
  summarize(
    pct_D_minus_R = list(pct_yes_D - pct_yes_R),
    avg_D = mean(pct_yes_D - pct_yes_R, na.rm = TRUE),
    n = n(),
    .by = topic
  ) |> 
  filter(n > 20, str_detect(topic, ",", negate = TRUE)) |> 
  arrange(desc(n)) |> 
  gt() |> 
  gt_plt_dist(pct_D_minus_R, type = "histogram", bw = 0.10) |> 
  fmt_number(avg_D) |> 
  tab_spanner("% Yes of Dems - % Yes of GOP", columns = c(pct_D_minus_R, avg_D)) |> 
  cols_label(
    topic = "Tax / Bond for ..",
    pct_D_minus_R = "Distribution",
    avg_D = "Average",
    n = md("_N_ <br>(Locales)")
  )
  
  
  
