library(tidyverse)
library(arrow)
library(glue)
library(readxl)

write_n <- function (tbl, path, dir = "paper/numbers") {
  tbl_n <- tbl |> 
    count() |> 
    collect()
  
  format(tbl_n$n[1], big.mark = ",") |> 
    write_lines(fs::path(dir, path))
}

# count voters by county
ds <- open_dataset("release")

dat <- ds |> 
  filter(office == "US PRESIDENT")


# Write ---

ds |> 
  write_n("N_records.tex")

ds |> 
  filter(party_detailed %in% c(
    "DEMOCRAT", "REPUBLICAN", "LIBERTARIAN", "GREEN"
  )) |> 
  distinct(office, district, candidate, party_detailed) |> 
  write_n("N_candidates.tex")

dat |> 
 write_n("N_voters.tex") 

dat |> 
  count() |> 
  collect() |> 
  pull(n) |> 
  scales::number(scale = 1e-6, accuracy = 0.1, suffix = " million") |> 
  write_lines("paper/numbers/N_voters_approx.tex")

dat |> 
  distinct(state, county_name) |> 
  write_n("N_counties.tex") 

dat |> 
  distinct(state) |> 
  write_n("N_states.tex") 