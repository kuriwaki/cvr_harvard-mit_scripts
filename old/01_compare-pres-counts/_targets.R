# Load packages required to define the pipeline:
library(targets)

# Set target options:
tar_option_set(
  # packages that your targets need to run
  packages = c(
    "tibble",
    "arrow",
    "haven",
    "dplyr",
    "tidyr",
    "purrr",
    "readr",
    "stringr",
    "glue",
    "fs"
  ),
  format = "rds"
)

# R/ functions
tar_source()

# targets
list(
  # Metadata -----
  # state county data
  tar_target(countycodes_path, "data/MEDSL/county-fips-codes.csv", format = "file"),
  tar_target(
    name = countycodes,
    {
      full <- tibble(st = state.abb, state = state.name) |>
        left_join(read_csv(countycodes_path))

      st0 <- full |>
        filter(st %in% statewide_states) |>
        mutate(st_fips = floor(county_fips / 1000)) |>
        distinct(st, state, st_fips) |>
        mutate(county_fips = st_fips*1000,
               county_name = "STATEWIDE") |>
        select(-st_fips)

      full |>
        filter(!st %in% is.na(statewide_states)) |>
        bind_rows(st0)
    }
  ),
  tar_target(
    name = countycode_out,
    write_csv(countycodes, "data/countycodes.csv")
  ),

  # statewide states
  tar_target(
    name = statewide_states,
    c("DC", "AK", "RI", "DE"),
  ),

  # CNN validation
  tar_target(cnn_counts_path, "data/val/cnn_county.csv", format = "file"),
  tar_target(
    name = val_counts,
    {
      init <- read_csv(cnn_counts_path)

      # collapse to state
      st0 <- init |>
        filter(st %in% statewide_states) |>
        mutate(st_fips = floor(county_fips / 1000)) |>
        group_by(st, state, st_fips, choice, wt = votes_reported, n = "votes_reported") |>
        mutate(county_fips = st_fips*1000,
               county_name = "STATEWIDE") |>
        select(-st_fips)

      init |>
        filter(!st %in% statewide_states) |>
        bind_rows(st0)
    }
  ),

  tar_target(
    name = val_counts_out,
    write_csv(val_counts, "data/validation-pres.csv")
  ),

  # MEDSL side
  tar_target(medsl_counts_path, "data/MEDSL/president_byCounty.csv", format = "file"),
  tar_target(
    name = medsl_counts,
    {
    medsl_fmt <- read_csv(medsl_counts_path) |>
      left_join(distinct(countycodes, st, state) |> mutate(state = str_to_upper(state)),
                by = "state") |>
      mutate(county_fips = as.numeric(glue("{state_fips}{county_fips}"))) |>
      select(-county_name)

    statewide_medsl <- medsl_fmt |>
      filter(st %in% statewide_states) |>
      group_by(st, state, state_fips, candidate) |>
      summarize(n = sum(n, na.rm = TRUE)) |>
      mutate(county_name = "STATEWIDE",
             county_fips = as.numeric(state_fips)*1000)

      # join with standard county name and state
    medsl_fmt |>
      filter(!st %in% statewide_states) |>
      bind_rows(statewide_medsl) |>
      left_join(mutate(countycodes, state = str_to_upper(state)),
                by = c("st", "state", "county_fips")) |>
      mutate(county_name = coalesce(county_name.x, county_name.y)) |>
      relocate(st, state) |>
      select(-state_fips, -proportion, -county_name.x, -county_name.y)
    }
  ),

  tar_target(
    name = medsl_notcounties,
    read_csv(medsl_counts_path) |>
      filter(is.na(county_fips)) |>
      write_csv("data/MEDSL/medsl_nocounty-data.csv")
  ),

  # party affiliation
  tar_target(snyder_cand_path, "~/Dropbox/CVR_Data_Shared/data_main/item_choice_info.dta", format = "file"),
  tar_target(
    snyder_cand,
    {
      read_dta(snyder_cand_path) |>
        select(state, county, column, choice_id, party)
    }
  ),

  # Our paths
  tar_target(
    name = snyder_paths,
    {
      fs::dir_ls("~/Dropbox/CVR_Data_Shared/data_main/STATA_long") |>
        str_subset("[A-Z]{2}_.+?_long")
    },
    format = "file_fast"
  ),

  tar_target(
    name = lewis_json_paths,
    {
      fs::dir_ls("~/Dropbox/CVR-JSON/", recurse = 2, regexp = "offices_votes.dta") |>
        str_subset("/CA/", negate = TRUE) # overlaps with Aleksandra's compiled file
    },
    format = "file_fast"
  ),

  tar_target(
    name = kuriwaki_conevska_paths,
    {
      main_dir <- "~/Dropbox/CVR_Data_Shared/data_main/not-snyder_long/"
      path(main_dir, c("AZ", "MD", "CA"), "offices_votes.dta")
    },
    format = "file_fast"
  ),

  # metadata for files to pull
  tar_target(
    name = paths_tbl,
    {
      c(snyder_paths, lewis_json_paths, kuriwaki_conevska_paths) |>
        str_subset("2022", negate = TRUE) |>
        str_subset("_prim_", negate = TRUE) |>
        str_subset("(was in CO by mistake?)", negate = TRUE) |>
        enframe(value = "path", name = "id") |>
        mutate(
          dataset_format = case_when(
            path %in% snyder_paths ~ "snyder_standard",
            path %in% c(kuriwaki_conevska_paths, lewis_json_paths) ~ "kuriwaki_standard",
          ),
          state = str_extract(path, "/([A-Z]{2})/") |>
            str_remove_all("/"))
    }
  ),

  # save metadata
  tar_target(
    name = paths_out,
    {
      write_csv(paths_tbl, "data/paths_harvard.csv")
    }
  ),

  # READ kuriwaki / conevska / lewis ----
  tar_target(
    name = pres_counts_kuriwaki,
    {
      paths_vec <- paths_tbl |>
        filter(dataset_format == "kuriwaki_standard") |>
        select(state, path) |>
        deframe()

      map(
        paths_vec,
        \(x) {
          read_dta(x) |>
            rename(any_of(c(candidate = "cand_code"))) |>
            filter(office %in% c("us_pres", "US_PRES")) |>
            rename(county_name = county) |>
            count(county_name, office, candidate, party)
        },
        .progress = TRUE) |>
        list_rbind(names_to = "st")
    }
  ),

  # READ snyder ----
  tar_target(
    name = pres_counts_snyder,
    {
      paths_vec <- paths_tbl |>
        filter(dataset_format == "snyder_standard") |>
        pull(path)

      names(paths_vec) <- path_file(paths_vec)

      map(
        paths_vec,
        \(x) {
          read_dta(x) |>
            rename(any_of(c(candidate = "choice", office = "item"))) |>
            filter(office %in% c("US_PRES")) |>
            count(office, column, choice_id, candidate)
        },
        .progress = TRUE) |>
        list_rbind(names_to = "filename") |>
        # list state + county
        separate_wider_delim(
          filename,
          delim = "_",
          too_many = "merge",
          names = c("state", "county")) |>
        # get state and county as displayed in Jim's data, then merge to get party
        mutate(county = str_remove(county, "_long.dta")) |>
        tidylog::left_join(snyder_cand, by = c("state", "county", "column", "choice_id")) |>
        select(st = state, county_name = county, candidate, party, n)
    }
  ),

  # cosmetic and county FIPS
  tar_target(
    pres_counts,
    {
      bind_rows(kuriwaki = pres_counts_kuriwaki, snyder = pres_counts_snyder,
                .id = "source") |>
        select(-office) |>
        mutate(
          # fix all county names at once
          county_name = fix_county_name(std_county_name(county_name)),
          party = recode(str_to_upper(party), REP = "R", DEM = "D")
        ) |>
        left_join(countycodes, by = c("st", "county_name")) |>
        mutate(across(where(is.character), \(x) na_if(x, ""))) |>
        relocate(st, state, matches("county"))
    }
  ),

  tar_target(
    comp,
    {
      medsl_county <- medsl_counts |>
        summarize(
          n_medsl = sum(n, na.rm = TRUE),
          nRD_medsl = sum(n * str_detect(candidate, "(TRUMP|BIDEN)"), na.rm = TRUE),
          .by = c(st, county_fips))

      cnn_county <- val_counts |>
        count(st, county_fips, county_name, wt = votes_reported, name = "nRD_cnn")

      # make unique, even if there are multiple sources
      pres_county <- pres_counts |>
        summarize(
          n_harvard = sum(n, na.rm = TRUE),
          nRD_harvard = sum(n * (party %in% c("R", "D")), na.rm = TRUE),
          .by = c(st, state, county_name, county_fips, source)) |>
        tidylog::distinct(st, county_fips, .keep_all = TRUE)

      pres_county |>
        tidylog::full_join(medsl_county, by = c("st", "county_fips")) |>
        tidylog::left_join(cnn_county, by = c("st", "county_fips")) |>
        mutate(county_name = coalesce(county_name.x, county_name.y)) |>
        select(-matches("(\\.x|\\.y)$")) |>
        relocate(starts_with("st"), county_fips, county_name, matches("nRD_"))
    }
  ),


  tar_target(
    name = out,
    {
      write_csv(comp, "data/harvard/release/president-counts.csv")
    }
  )
)
