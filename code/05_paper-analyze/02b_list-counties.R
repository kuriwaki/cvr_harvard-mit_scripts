library(tidyverse)
library(arrow)
library(fs)
library(glue)
library(writexl)
source("00_paths.R")

# count voters by county
dat <- open_dataset(path(PATH_parq, "release")) |>
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
         county = recode(county, Statewide = "(Statewide)"),
         county = recode(county, Mchenry = "McHenry",
                         Dekalb = "DeKalb", Mcduffie = "McDuffie")
         ) |>
  arrange(state, county)

rows_list <- list()

# line by line so we can do multicols ----
st_counter <- ""

sink("tables/tab_counties_text.tex")

for (i in 1:nrow(dat_fmt)) {
  # new state
  if (dat_fmt$state[i] != st_counter) {
    st <- dat_fmt$state[i]

    if (st != dat_fmt$state[1]) {
      cat("\\\\", "\n")
      rows_list <- append(rows_list, list(c("")))
    }

    cat(glue("\\textbf{[st]}\\\\", .open = "[", .close = "]"), "\n")
    rows_list <- append(rows_list, st)
    cat("{\\raggedright \\itshape ")
    if (!st %in% c("Alaska", "Delaware", "District of Columbia", "Rhode Island")) {
      cat(cli::pluralize("{dat_fmt$n_counties[i]} count{?y/ies}, {dat_fmt$n_voters[i]} voters \\par}"), "\n")
      rows_list <- append(rows_list, cli::pluralize("{dat_fmt$n_counties[i]} count{?y/ies}, {dat_fmt$n_voters[i]} voters"))
    } else {
      cat(glue("{dat_fmt$n_voters[i]} voters \\par}"), "\n")
      rows_list <- append(rows_list, glue("{dat_fmt$n_voters[i]} voters}"))
    }

    st_counter <- st
  }

  # print county
  cat(dat_fmt$county[i], "\\\\\n")
  rows_list <- append(rows_list, list(dat_fmt$county[i]))
}

sink()

output_df <- tibble(x = unlist(rows_list))
write_xlsx(output_df, "tables/table_04.xlsx")

