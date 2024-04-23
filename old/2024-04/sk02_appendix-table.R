library(tidyverse)
library(haven)
library(fs)

numdir <- "~/Dropbox/CVR_papers/all-politics-national/numbers/"
d1 <- read_dta("tmp_local_partisan_races_merged.dta")

d1 |> 
  filter(str_detect(place, "BALTIMORE"),
         item == "MAYOR") |> 
  select(state, place, item, N, matches("choice"))
