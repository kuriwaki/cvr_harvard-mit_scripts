#' finer grained classification
categorize_diff <- function(tbl) {

  tbl |>
    filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN", "LIBERTARIAN")) |>
    filter(!is.na(votes_h) & !is.na(votes_m)) |>
    mutate(
      diff_h  = votes_v - votes_h,
      diff_m  = votes_v - votes_m,
      diff_hm = votes_h - votes_m) |>
    summarize(
      across(matches("(diff_|votes_)"), sum),
      .by = c(state, county_name, office, party_detailed)) |>
    summarize(
      color2_h = case_when(
        all(diff_h == 0) & all(diff_h == 0) ~ "0 difference",
        all(abs(diff_h/votes_v) < 0.01) & all(abs(diff_h/votes_v) < 0.01) & any(diff_h != 0) ~ "any < 1% mismatch",
        all(diff_h/votes_v < 0.05) & all(diff_h/votes_v < 0.05) & any(abs(diff_h/votes_v) >= 0.01) ~ "any < 5% mismatch",
        all(diff_h/votes_v < 0.10) & all(diff_h/votes_v < 0.10) & any(abs(diff_h/votes_v) >= 0.05) ~ "any < 10% mismatch",
        .default = "red"
      ),
      .by = c(state, county_name)
    ) |>
    mutate(color2_h = factor(color2_h, levels = c("0 difference", "any < 1% mismatch", "any < 5% mismatch", "any < 10% mismatch", "red")))
}

# |>
#   writexl::write_xlsx("~/Dropbox/CVR_parquet/combined/county-classifications_finer.xlsx")
#  |>
#   arrange(color2) |>
#   count(color2)
