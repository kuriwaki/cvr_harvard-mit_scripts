#' finer grained classification
categorize_diff <- function(tbl, var, newvar, candvar) {
  tbl |>
    filter(party_detailed %in% c("DEMOCRAT", "REPUBLICAN", "LIBERTARIAN")) |>
    filter(!is.na({{candvar}}) | !is.na(candidate_v)) |>
    mutate(
      var_missing = is.na({{var}}),
      {{var}} := replace_na({{var}}, 0), # is missing when others are present, change to 0
      diff  = votes_v - {{var}},
      diffm = votes_h - votes_m) |>
    summarize(
      across(matches("(diff|votes_)"), \(x) sum(x, na.rm = FALSE)), # NAs should not occur but flag when it does
      office_miss = any(var_missing),
      .by = c(state, county_name, office, party_detailed)) |>
    mutate(adiff = abs(diff/votes_v)) |>
    summarize(
      {{newvar}} := case_when(
        all(diff == 0, na.rm = FALSE) ~ "0 difference",
        all(!office_miss) & all(adiff < 0.01, na.rm = FALSE) & all(adiff <  0.01, na.rm = FALSE) & any(diff != 0, na.rm = FALSE) ~ "any < 1% mismatch",
        all(!office_miss) & all(adiff < 0.05, na.rm = FALSE) & any(adiff >= 0.01, na.rm = FALSE) ~ "any < 5% mismatch",
        all(!office_miss) & all(adiff < 0.10, na.rm = FALSE) & any(adiff >= 0.05, na.rm = FALSE) ~ "any < 10% mismatch",
        all(!office_miss) & any(adiff > 0.10, na.rm = FALSE) ~ "red",
        any(office_miss) & any(!office_miss) ~ "candidate missing",
        all(office_miss) ~ "not collected",
        .default = "unclassified"
      ),
      .by = c(state, county_name)
    ) |>
    mutate({{newvar}} := factor({{newvar}}, levels = c("0 difference",
                                                       "any < 1% mismatch",
                                                       "any < 5% mismatch",
                                                       "any < 10% mismatch",
                                                       "candidate missing",
                                                       "not collected",
                                                       "red",
                                                       "unclassified")))
}
