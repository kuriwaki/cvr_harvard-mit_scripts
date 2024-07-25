library(tidyverse)
library(arrow)
library(glue)
library(readxl)

# count voters by county
dat <- open_dataset("release") |> 
  filter(office == "US PRESIDENT") |> 
  count(state, county_name, name = "voters_pres") |> 
  collect() |> 
  mutate(county_name = replace(
    county_name, 
    state %in% c("RHODE ISLAND", "DELAWARE", "DISTRICT OF COLUMBIA"),
    "STATEWIDE"))

# Subset to releasable counties, reformat county name
dat_fmt <- dat |> 
  mutate(
    n_counties = n(),
    n_voters = scales::comma(sum(voters_pres)),
    .by = state
  ) |> 
  mutate(state = str_to_title(state),
         county = str_to_title(county_name),
         county = recode(county, Statewide = "(Statewide)")) |> 
  arrange(state, county)


# line by line so we can do multicols ----
st_counter <- ""

sink("tables/tab_counties_text.tex")

for (i in 1:nrow(dat_fmt)) {
  # new state
  if (dat_fmt$state[i] != st_counter) {
    st <- dat_fmt$state[i]
    
    if (st != dat_fmt$state[1]) 
      cat("\\\\", "\n")
    
    cat(glue("\\textbf{[st]}\\\\", .open = "[", .close = "]"), "\n")
    cat("{\\raggedright \\itshape ")
    if (!st %in% c("Alaska", "Delaware", "District of Columbia", "Rhode Island"))
      cat(cli::pluralize("{dat_fmt$n_counties[i]} count{?y/ies}, "))
    
    cat(glue("{dat_fmt$n_voters[i]} voters \\par}"), "\n")
    st_counter <- st
  }
  
  # print county
  cat(dat_fmt$county[i], "\\\\\n")
}
sink()
