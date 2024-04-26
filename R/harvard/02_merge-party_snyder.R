library(arrow)
library(tidyverse)
library(fs)
library(duckplyr)


# Output locations
PATH_projdir <- "~/Dropbox/CVR_Data_Shared/data_main"
PATH_long <- "~/Downloads/stata_init/*/*/*.parquet" # for duckplyr::duckplyr_df_from_parquet()
PATH_long <- "~/Downloads/stata_init/" # for arrow::open_dataset()
PATH_merged <- "~/Downloads/stata_long" # Change to Dropbox CVR_Data_Shared when final

# Datasets
## item_info0 has
meta <- open_dataset(
  path(PATH_projdir, "to-parquet", "item_choice_info"))
ds_orig <- open_dataset(PATH_long)


# Main merge ----

# Have to do this state by state since my R crashes otherwise
states_vec <- distinct(ds_orig, state) |> pull(state, as_vector = TRUE)

for (st in states_vec) {

  ds <- ds_orig |>
    filter(state == st) |>
    inner_join(
      select(meta, state:dist, choice_id, party, level,
             nonpartisan, unexp_term,
             incumbent, spending, measure, multi_county, num_votes),
      by = c("state", "county", "column", "item", "choice_id"),
      relationship = "many-to-one")

  # Party ID ---
  # follows Snyder "MAKE PARTISANSHIP" in `analysis_all_politics_partisan.do`
  ds_pid <- ds |>
    filter(level == "N") |>
    mutate(
      D = as.numeric(party == "DEM"),
      R = as.numeric(party == "REP"),
    ) |>
    summarize(
      D = sum(D),
      R = sum(R),
      .by = c(state, county, cvr_id)
    ) |>
    mutate(
      pid_num = case_when(
        D > 0 & R == 0 ~ 1,
        D > 0 & R > 0  ~ 0,
        D == 0 & R > 0 ~ -1,
      ),
      pid = case_when(
        pid_num == 1 ~ "DEM",
        pid_num == 0 ~ "SPL",
        pid_num == -1 ~ "REP"
      )) |>
    select(state, county, cvr_id, pid, pid_num)


  # Write ----
  ds |>
    # merge national party
    left_join(
      ds_pid,
      by = c("state", "county", "cvr_id"),
      relationship = "one-to-one") |>
    relocate(pid, pid_num, .after = cvr_id) |>
    group_by(state, county) |> # specifies partition
    write_dataset(
      path = PATH_merged,
      format = "parquet",
      existing_data_behavior = "delete_matching")
}
