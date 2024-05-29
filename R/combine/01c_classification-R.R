#' finer grained classification
categorize_diff <- function(tbl, var, newvar) {

  tbl |>
    filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN", "LIBERTARIAN")) |>
    mutate(
      {{var}} := replace_na({{var}}, 0), # is missing when others are present, change to 0
      diff  = votes_v - {{var}},
      diffm = votes_h - votes_m) |>
    summarize(
      across(matches("(diff|votes_)"), \(x) sum(x, na.rm = FALSE)), # NAs should not occur but flag when it does
      .by = c(state, county_name, office, party_detailed)) |>
    summarize(
      {{newvar}} := case_when(
        all(diff == 0) & all(diff == 0) ~ "0 difference",
        all(abs(diff/votes_v) < 0.01) & all(abs(diff/votes_v) < 0.01) & any(diff != 0) ~ "any < 1% mismatch",
        all(diff/votes_v < 0.05) & all(diff/votes_v < 0.05) & any(abs(diff/votes_v) >= 0.01) ~ "any < 5% mismatch",
        all(diff/votes_v < 0.10) & all(diff/votes_v < 0.10) & any(abs(diff/votes_v) >= 0.05) ~ "any < 10% mismatch",
        any(is.na({{var}})) & any(!is.na({{var}})) ~ "candidate missing",
        all(is.na({{var}})) ~ "not collected",
        .default = "red"
      ),
      .by = c(state, county_name)
    ) |>
    mutate({{newvar}} := factor({{newvar}}, levels = c("0 difference",
                                                       "any < 1% mismatch",
                                                       "any < 5% mismatch",
                                                       "any < 10% mismatch",
                                                       "candidate missing",
                                                       "not collected",
                                                       "red")))
}

# |>
#   writexl::write_xlsx("~/Dropbox/CVR_parquet/combined/county-classifications_finer.xlsx")
#  |>
#   arrange(color2) |>
#   count(color2)
