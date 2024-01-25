# US House
sum_ushou <- function(tbl) {
  tbl |>
    filter(office == "US_REP") |>
    tidylog::filter(!is.na(dist)) |>
    tidylog::filter(!is.na(st)) |>
    count(st, dist, candidate, party, wt = N) |>
    mutate(dist = str_remove(dist, "^0+")) |>
    mutate(cd = ccesMRPprep::to_cd(st, dist)) |>
    filter(str_detect(cd, "NA", negate = TRUE)) |>
    summarize(
      N_REP = sum(n * (party == "REP"), na.rm = TRUE),
      N_DEM = sum(n * (party == "DEM"), na.rm = TRUE),
      .by = c(cd)) |>
    tidylog::filter(N_REP > 0 | N_DEM > 0)
}



sum_ussen <- function(tbl) {
  tbl |>
    filter(office == "US_SEN") |>
    tidylog::filter(!is.na(st)) |>
    count(st, candidate, party, wt = N) |>
    summarize(
      N_REP = sum(n * (party == "REP"), na.rm = TRUE),
      N_DEM = sum(n * (party == "DEM"), na.rm = TRUE),
      .by = c(st)) |>
    tidylog::filter(N_REP > 0 | N_DEM > 0)
}
