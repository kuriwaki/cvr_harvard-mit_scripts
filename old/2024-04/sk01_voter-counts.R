library(tidyverse)
library(haven)
library(fs)

numdir <- "~/Dropbox/CVR_papers/all-politics-national/numbers/"
d0 <- read_dta("tmp0.dta")

d0_item <- d0 |> 
  count(item_id, state, item, dist, place, 
        level, nonpartisan, spending,
        unexp_term, 
        wt = votes,
        name = "total_voters")


pull_write <- function(tbl, path, dir = numdir){
  tbl |> 
    pull(total_voters) |> 
    sum() |> 
    scales::comma() |> 
    write_lines(fs::path(numdir, path))
}



d0_item |> 
  filter(item == "US_PRES") |> 
  pull_write("votersN_pres.tex")


d0_item |> 
  mutate(cat = case_when(
    item == "US_PRES" ~ "President",
    item == "US_REP" ~ "U.S. House",
    item == "US_SEN" ~ "U.S. Senate",
    item == "ST_REP" ~ "State House",
    item == "ST_SEN" ~ "State Senate",
    level == "S" & nonpartisan == 0 ~ "State Partisan",
    level == "S" & nonpartisan == 1 ~ "State Non-partisan",
    level == "L" & nonpartisan == 0 ~ "Local Partisan",
    level == "L" & nonpartisan == 1 ~ "Local Non-Partisan",
    level == "L" & is.na(nonpartisan) & spending == 1 ~ "Local Ballot Measure Spending",
    level == "L" & is.na(nonpartisan) & spending == 0 ~ "Local Ballot Measure Non-Spending",
  )) |> 
  summarize(
    n_states = n_distinct(state),
    n_items = n_distinct(item_id),
    n_voters = sum(total_voters),
    .by = cat
  )


# By Office
ushou_dat <- d0 |> 
  filter(item == "US_REP") |> 
  summarize(
    dem_votes = sum(votes * (choice == "DEM")),
    rep_votes = sum(votes * (choice == "REP")),
    totalvotes = sum(votes), 
    .by = c(state, dist))

pres_dat <-  d0 |> 
  filter(item == "US_PRES") |> 
  summarize(
    biden_votes = sum(votes * (choice == "DEM")),
    trump_votes = sum(votes * (choice == "REP")),
    totalvotes = sum(votes), 
    .by = c(state, county))

d0 |> 
  filter(
    item == "CO_BOE" |
    str_sub(item, -4, 4) == " BOE" | 
      str_detect(item, "( BOE|SCHOOL BOARD|SCHOOL GOV BOARD|SCHOOL DIST|SCHOOL TRUSTEE|SCHOOL SYSTEM|SCHOOL COMM|UNIFIED|EDUC)")
  ) |> 
  select(state, item, )

pres_pop <- pres_dat |> 
  summarize(across(where(is.numeric), sum))


nrow(ushou_dat) |> 
  write_lines(path(numdir, "congdistN_all.tex"))

n_distinct(pres_dat$state) |> 
  write_lines(path(numdir, "statesN_all.tex"))
  
nrow(pres_dat) |> 
  write_lines(path(numdir, "countiesN_all.tex"))

with(pres_pop, 100*biden_votes / (trump_votes + biden_votes)) |> 
  round(digits = 2) |> 
  write_lines(path(numdir, "bidenshare_all.tex"))

with(pres_pop, (trump_votes + biden_votes)) |> 
  scales::comma() |> 
  write_lines(path(numdir, "bidentrump_all.tex"))

81283501 / (81283501 + 74223975) # https://en.wikipedia.org/wiki/2020_United_States_presidential_election
