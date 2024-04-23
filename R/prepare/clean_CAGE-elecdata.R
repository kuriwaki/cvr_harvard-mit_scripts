library(dataverse)


elec_results <- get_dataframe_by_name(
  "candidates_2006-2020.tab",
  server = "dataverse.harvard.edu",
  dataset = "10.7910/DVN/DGDRDT",
  original = TRUE,
  .f = haven::read_dta
)



elec_wide_H <- elec_results |>
  filter(office %in% c("H"), year == 2020) |>
  mutate(cd = to_cd(state, dist)) |>
  summarize(
    N_R_all = sum(candidatevotes * (party == "R"), na.rm = TRUE),
    N_D_all = sum(candidatevotes * (party == "D"), na.rm = TRUE),
    .by = c(office, cd)
  ) |>
  transmute(
    office,
    cd,
    shareD_all = N_D_all / (N_D_all + N_R_all),
    N_all = N_R_all + N_D_all,
  )

elec_wide_S <- elec_results |>
  filter(office %in% c("S"), year == 2020, type == "G") |>
  summarize(
    N_R_all = sum(candidatevotes * (party == "R"), na.rm = TRUE),
    N_D_all = sum(candidatevotes * (party == "D"), na.rm = TRUE),
    .by = c(office, state)
  ) |>
  transmute(
    office,
    st = state,
    shareD_all = N_D_all / (N_D_all + N_R_all),
    N_all = N_R_all + N_D_all,
  )



elec_wide_H |>
  write_csv("data/val/ush2020_by-cd.csv")
