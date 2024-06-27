#' Fully clean CVR data
#'
#' @param df A semi-processed data-frame
#' @inheritParams get_contests
#' @param type The type of data-frame
#' @param contests The data-frame of manually categorized contests
#'
#' @return The cleaned data
clean_data <- function(df, st, cnty, type, contests) {
  if (type == "delim") {
    d <- df |>
      # rename with a fuzzy match, just trying to get the precinct
      rename(any_of(RENAME_COLS)) |>
      # drop all of the unnecessary columns
      select(-any_of(DROP_COLS)) |>
      # manually define the county, state, and cvr_id column
      # extract county and state from the path
      # just define cvr_id as 1:n()
      mutate(
        county_name = cnty,
        state = st,
        cvr_id = 1:n()
      ) |>
      pivot_longer(
        cols = -any_of(c("state", "county_name", "cvr_id", "precinct")),
        names_to = "contest",
        values_to = "candidate",
        values_drop_na = TRUE
      ) |>
      left_join(contests, join_by(state, county_name, contest)) |>
      # this block identifies undervotes in the 1/0 CVR format
      # first, we identify if a voter cast a vote in this race
      # this step keeps the data in an extra long format
      mutate(
        voted = ifelse(
          all(candidate.x %in% c("0", "X") | str_detect(candidate.x, regex("undervote", ignore_case=TRUE))),
          0,
          1
        ),
        .by = c("cvr_id", "office", "district")
      ) |>
      # if they voted, then we just use their lookup table candidate choice
      # if they didn't vote, they get assigned to undervote
      # then, we replace all the 0s with NA, now that we've identified undervote
      #
      # this also deals with CVRs that have contests as columns and cands as cells
      mutate(
        candidate = case_when(
          voted == 0 & candidate.x == "X" ~ NA_character_,
          voted == 0 ~ "undervote",
          candidate.x == "0" ~ NA_character_,
          .default = coalesce(candidate.y, candidate.x)
        ),
      )
  } 
  else if (type == "json") {
    d <- df |>
      mutate(
        county_name = cnty,
        state = st
      ) |>
      left_join(contests, join_by(state, county_name, contest)) |>
      # fix some of the multi-columns
      mutate(
        magnitude = coalesce(magnitude.x, magnitude.y),
        party_detailed = coalesce(party_detailed.x, party_detailed.y),
        candidate = coalesce(candidate.y, candidate.x)
      )
  } 
  else if (type == "xml") {
    d <- df |>
      mutate(
        county_name = cnty,
        state = st
      ) |>
      left_join(contests, join_by(state, county_name, contest)) |>
      # manually define the county and state
      mutate(
        candidate = coalesce(candidate.y, candidate.x)
      )
  }

  d |>
    # sometimes there are invalid contests we only catch in the making of the lookup table
    # those contests have a blank office type, so we just drop those here
    # also drop all the NAs for candidate (these were the 0s in the delim files)
    drop_na(office, candidate) |>
    # one last sanity check for distinctness
    distinct(cvr_id, office, district, candidate, .keep_all = TRUE) |>
    mutate(
      # clean candidate up a bit at the beginning, so we can catch more party information
      candidate = str_remove_all(candidate, "^$|^NA$|^N/A$|\\([^)]*\\)$|[\\p{Mn}]|[[:punct:]]"),
      candidate = stri_trans_nfd(candidate),
      candidate = str_to_upper(candidate),
      # detect party where it is not assigned manually
      party_detailed = case_when(
        !is.na(party_detailed) ~ str_to_upper(party_detailed),
        str_detect(candidate, regex("^CPF ", ignore_case = TRUE)) ~ "CONSTITUTION",
        str_detect(candidate, regex("^GRN ", ignore_case = TRUE)) ~ "GREEN",
        str_detect(candidate, regex("^LBT |^LIB | LIB$|^LPN | LPN$|^NMD ", ignore_case = TRUE)) ~ "LIBERTARIAN",
        str_detect(candidate, regex("^DEM | DEM$|\\(DEM\\)$|^DFL | DFL$", ignore_case = TRUE)) ~ "DEMOCRAT",
        str_detect(candidate, regex("^REP | REP$|\\(REP\\)$", ignore_case = TRUE)) ~ "REPUBLICAN",
        str_detect(candidate, regex("^PGP ", ignore_case = TRUE)) ~ "SOCIALIST",
        str_detect(candidate, regex("^NPA ", ignore_case = TRUE)) ~ "NO PARTY AFFILIATION",
        str_detect(candidate, regex("^PRO ", ignore_case = TRUE)) ~ "PROGRESSIVE",
        str_detect(candidate, regex("^IND |^IAP ", ignore_case = TRUE)) ~ "INDEPENDENT",
        str_detect(candidate, regex("^GLC ", ignore_case = TRUE)) ~ "GRASSROOTS-LEGALIZE CANNABIS",
        str_detect(candidate, regex("^NME ", ignore_case = TRUE)) ~ "END THE CORRUPTION",
        str_detect(candidate, regex("Write", ignore_case = TRUE)) ~ "OTHER",
        str_detect(candidate, regex("undervote|overvote|No image found", ignore_case=TRUE)) ~ NA_character_,
        is.na(candidate) ~ NA_character_,
        .default = party_detailed
      ),
      candidate = case_when(
        office == "US PRESIDENT" & str_detect(candidate, regex("Biden", ignore_case = TRUE)) ~ "JOSEPH R BIDEN",
        office == "US PRESIDENT" & str_detect(candidate, regex("Trump", ignore_case = TRUE)) ~ "DONALD J TRUMP",
        office == "US PRESIDENT" & str_detect(candidate, regex("Jorgensen", ignore_case = TRUE)) ~ "JO JORGENSEN",
        office == "US PRESIDENT" & str_detect(candidate, regex("Hawkins", ignore_case = TRUE)) ~ "HOWIE HAWKINS",
        office == "US PRESIDENT" & str_detect(candidate, regex("Pierce", ignore_case = TRUE)) ~ "BROCK PIERCE",
        office == "US PRESIDENT" & str_detect(candidate, regex("Blankenship", ignore_case = TRUE)) ~ "DON BLANKENSHIP",
        office == "US PRESIDENT" & str_detect(candidate, regex("Janos", ignore_case = TRUE)) ~ "JAMES G JANOS",
        office == "US PRESIDENT" & str_detect(candidate, regex("Fuente|Rocky", ignore_case = TRUE)) ~ "ROCQUE DE LA FUENTE",
        office == "US PRESIDENT" & str_detect(candidate, regex("Kanye|West", ignore_case = TRUE)) ~ "KANYE WEST",
        office == "US PRESIDENT" & str_detect(candidate, regex("Carroll", ignore_case = TRUE)) ~ "BRIAN T CARROLL",
        office == "US PRESIDENT" & str_detect(candidate, regex("Gloria|Riva", ignore_case = TRUE)) ~ "GLORIA LA RIVA",
        .default = str_remove_all(candidate, regex("^CPF |^GRN |^LBT |^DEM | DEM$|^REP | REP$|^PGP |^NPA |^PRO |^IND|^LIB | LIB$|\\(REP\\)$|\\(DEM\\)$|No image found|^DFL | DFL$|^GLC |^LPN | LPN$|^IAP |^NMD |^NME ", ignore_case = TRUE))
      ),
      party_detailed = case_when(
        office == "US PRESIDENT" & candidate == "JOSEPH R BIDEN" ~ "DEMOCRAT",
        office == "US PRESIDENT" & candidate == "DONALD J TRUMP" ~ "REPUBLICAN",
        office == "US PRESIDENT" & candidate == "JO JORGENSEN" ~ "LIBERTARIAN",
        office == "US PRESIDENT" & candidate == "BROCK PIERCE" ~ "INDEPENDENT",
        office == "US PRESIDENT" & candidate == "DON BLANKENSHIP" ~ "CONSTITUTION",
        office == "US PRESIDENT" & candidate == "ROCQUE DE LA FUENTE" ~ "REFORM",
        office == "US PRESIDENT" & candidate == "HOWIE HAWKINS" ~ "GREEN",
        str_detect(candidate, regex("undervote|overvote|No image found", ignore_case=TRUE)) ~ NA_character_,
        .default = party_detailed
      ),
      # this regex removes several things:
      # - any empty strings
      # - any weird NA characters
      # - content that is between parens
      # - remove any non-marking unicode characters, such as diacritics
      # - remove punctuation
      # - squish to remove extra spaces
      candidate = str_remove_all(candidate, "^$|^NA$|^N/A$|\\([^)]*\\)$|[\\p{Mn}]|[[:punct:]]"),
      candidate = str_squish(candidate),
      candidate = if_else(str_detect(candidate, "WRITE"), "WRITEIN", candidate),
      # special field for Alaska
      jurisdiction_name = ifelse(st == "ALASKA", str_to_upper(str_extract(precinct, "District \\d+")), county_name)
    ) |>
    select(
      # generic function since some columns might be missing in some states
      # for odd reasons. Most likely to be missing is `precinct`
      any_of(c(
        "state", "county_name", "jurisdiction_name", "cvr_id", "precinct", "office", "district",
        "candidate", "party_detailed", "magnitude", "contest"
      ))
    )
}

#' Write data to the cleaned directory
#'
#' @param df the cleaned data
#' @inheritParams get_contests
#'
#' @return The path to each cleaned file, for targets to track
write_data <- function(df, state, county_name){
  
  county_name = ifelse(county_name == "NA" | is.na(county_name), "", county_name)
  county_name = str_replace_all(county_name, fixed(" "), fixed("%20"))
  county_name = str_replace_all(county_name, fixed("'"), fixed("%27"))
  state = str_replace_all(state, fixed(" "), fixed("%20"))
  state = str_replace_all(state, fixed("'"), fixed("%27"))
  
  write_dataset(df, CLEAN_DIR, format = "parquet", partitioning = c("state", "county_name"))
  
  sprintf("%s/state=%s/county_name=%s/part-0.parquet", CLEAN_DIR, state, county_name)
  
}

################################
## Processing Functions
################################

process_delim <- function(path, state = NA, county_name = NA, contests = NULL, n = Inf) {
  if (is_header(path)) {
    d <- header_processor(path)
  } else {
    if (str_detect(path, "csv$|CSV$")) {
      d <- read_csv(path, col_types = cols(.default = col_character()), n_max = n, name_repair = "unique_quiet")
    } else if (str_detect(path, "xls$|xlsx$|XLS$|XLSX$")) {
      d <- read_excel(path, col_types = "text", n_max = n, .name_repair = "unique_quiet")
    }

    colnames(d) <- iconv(colnames(d), to = "UTF-8", sub = "")
  }

  # if column names only
  if (n < Inf) {
    return(d)
  }

  clean_data(d, state, county_name, type = "delim", contests = contests) |> 
    write_data(state, county_name)
}

preprocess_json <- function(dir, contest_only = FALSE){
  
  lookup_district <- read_json(str_c(dir, "/DistrictManifest.json")) |>
    as_tibble() |>
    hoist(List,
      district = "Description",
      id = "Id"
    ) |>
    select(-Version, -List)
  
  lookup_contests <- read_json(str_c(dir, "/ContestManifest.json")) |>
    as_tibble() |>
    hoist(List,
      contest = "Description",
      district_of_contest = "DistrictId",
      id = "Id"
    ) |>
    select(-Version, -List) |>
    left_join(lookup_district, join_by(district_of_contest == id))
  
  if (contest_only) {
    return(lookup_contests$contest)
  }
  
  lookup_candidates <- read_json(str_c(dir, "/CandidateManifest.json")) |>
    as_tibble() |>
    hoist(List,
      candidate = "Description",
      candidate_type = "Type",
      id = "Id"
    ) |>
    select(-Version, -List)
  
  lookup_party <- read_json(str_c(dir, "/PartyManifest.json")) |>
    as_tibble() |>
    hoist(List,
      party = "Description",
      id = "Id"
    ) |>
    select(-Version, -List) |>
    mutate(party = case_when(
      str_detect(party, regex("REP", ignore_case = TRUE)) ~ "REPUBLICAN",
      str_detect(party, regex("DEM|Democrat|Democratic", ignore_case = TRUE)) ~ "DEMOCRAT",
      str_detect(party, regex("LBT|Libertarian|Lib|LPN", ignore_case = TRUE)) ~ "LIBERTARIAN",
      str_detect(party, regex("IND|IAP|IPN", ignore_case = TRUE)) ~ "INDEPENDENT",
      str_detect(party, regex("NON|NPN|Nonpartisan", ignore_case = TRUE)) ~ "NONPARTISAN",
      .default = str_to_upper(party)
    ))
  
  lookup_precinct_portion <- read_json(str_c(dir, "/PrecinctPortionManifest.json")) |>
    as_tibble() |>
    hoist(List,
      precinct_portion = "Description",
      precinct = "PrecinctId",
      id = "Id"
    ) |>
    select(-Version, -List)
  
  clean_json <- function(path) {
  
    read_json(path) |>
      as_tibble() |>
      unnest_wider(col = Sessions) |>
      bind_rows(
        tibble(
          TabulatorId = integer(),
          BatchId = integer(),
          RecordId = integer(),
          CountingGroupId = integer(),
          Original = list(),
          Modified = list()
        )
      ) |> 
      mutate(Original = coalesce(Modified, Original)) |>
      hoist(
        .col = Original,
        precinctportion_id = "PrecinctPortionId",
        cards = list("Cards", 1L)
      ) |>
      unnest_wider(cards) |>
      select(-Id, -PaperIndex, -OutstackConditionIds) |>
      unnest_longer(Contests) |>
      hoist(
        .col = Contests,
        contest_id = "Id",
        marks = "Marks",
        overvotes = "Overvotes"
      ) |>
      unnest_longer(marks) |>
      hoist(
        .col = marks,
        candidate_id = "CandidateId",
        party_id = "PartyId",
        magnitude = "Rank",
        is_vote = "IsVote",
        is_ambiguous = "IsAmbiguous"
      ) |>
      filter(is_vote, !is_ambiguous) |>
      select(TabulatorId, BatchId, RecordId, precinctportion_id:magnitude)
  }
  
  files <- list.files(path = dir, pattern = "CvrExport|CVRExport", full.names = TRUE, recursive = TRUE)
  
  plan(multisession, workers = 16)
  
  d <- future_map(files, possibly(clean_json, quiet = FALSE)) |>
    list_rbind() |>
    mutate(
      cvr_id = cur_group_id(),
      .by = c(TabulatorId, BatchId, RecordId)
    ) |>
    left_join(lookup_candidates, by = c("candidate_id" = "id")) |>
    left_join(lookup_contests, by = c("contest_id" = "id")) |>
    left_join(lookup_party, by = c("party_id" = "id")) |>
    left_join(lookup_precinct_portion, by = c("precinctportion_id" = "id")) |>
    select(-precinct) |>
    rename(
      party_detailed = party,
      precinct = precinct_portion
    ) |>
    mutate(magnitude = as.character(magnitude)) |>
    select(cvr_id, precinct, contest, candidate, party_detailed, magnitude)
  
  plan(sequential)
  
  return(d)
  
}

process_json <- function(df, state = NA, county_name = NA, contests = NULL) {
  
  clean_data(df, state, county_name, type = "json", contests) |> 
    write_data(state, county_name)
}

preprocess_xml <- function(dir, contest_only = FALSE){
  
  xml_parser <- function(path, i) {
    x <- read_xml(path)
    
    df <- tibble(d = as_list(x)) |>
      unnest_wider(d) |>
      select(Contests, PrecinctSplit) |>
      unnest_longer(Contests) |>
      hoist(
        .col = Contests,
        contest = list("Name", 1L),
        writein_name = list("Options", 1L, "WriteInData", "Text", 1L),
        candidate_name = list("Options", 1L, "Name", 1L)
      ) |>
      hoist(
        .col = PrecinctSplit,
        precinct = list("Name", 1L)
      ) |>
      mutate(
        writein_name = if_else(!is.na(writein_name), "WRITEIN", writein_name),
        candidate = coalesce(writein_name, candidate_name, "undervote"),
        cvr_id = i
      ) |>
      select(cvr_id, contest, candidate, precinct)
    
    if (contest_only) {
      return(distinct(df, contest))
    } else {
      return(df)
    }
  }
  
  files <- list.files(
    path = dir,
    pattern = "*.xml",
    recursive = TRUE,
    full.names = TRUE
  )
  
  plan(multisession, workers = 4)
  
  xmls <- future_imap(files, xml_parser) |> list_rbind()
  
  plan(sequential)
  
  if (contest_only){
    distinct(xmls, contest) |> pull() |> unname()
  } else {
    return(xmls)
  }
  
}

process_xml <- function(df, state = NA, county_name = NA, contests = NULL) {
  
  clean_data(df, state, county_name, "xml", contests) |> write_data(state, county_name)
  
}

process_special <- function(path, s, c, contests) {
  if (s == "TEXAS" & c == "DENTON") {
    d <- read_csv(path,
      col_types = cols(.default = "c"),
      col_select = c("cid", "Precinct", "Race", "Candidate")
    ) |>
      rename(
        contest = Race,
        candidate = Candidate,
        precinct = Precinct,
        cvr_id = cid
      ) |>
      left_join(contests, join_by(contest)) |>
      drop_na(office) |>
      mutate(
        voted = ifelse(
          all(candidate.x == "0" | str_detect(candidate.x, regex("undervote", ignore_case=TRUE))),
          0,
          1
        ),
        .by = c("cvr_id", "office", "district")
      ) |> 
      mutate(
        candidate.x = if_else(voted == 1, candidate.x, "undervote"),
        candidate = case_when(
          str_detect(candidate.x, regex("undervote", ignore_case=TRUE)) ~ "undervote",
          candidate.x == "0" ~ NA_character_,
          .default = coalesce(candidate.y, candidate.x)
        ),
      ) |>
      drop_na(candidate) |>
      distinct(cvr_id, office, district, candidate, .keep_all = TRUE) |>
      mutate(
        party_detailed = case_when(
          !is.na(party_detailed) ~ str_to_upper(party_detailed),
          str_detect(candidate, regex("^CPF ", ignore_case = TRUE)) ~ "CONSTITUTION",
          str_detect(candidate, regex("^GRN ", ignore_case = TRUE)) ~ "GREEN",
          str_detect(candidate, regex("^LBT |^LIB | LIB$| ^LPN| LPN$", ignore_case = TRUE)) ~ "LIBERTARIAN",
          str_detect(candidate, regex("^DEM | DEM$|\\(DEM\\)$|^DFL | DFL$", ignore_case = TRUE)) ~ "DEMOCRAT",
          str_detect(candidate, regex("^REP | REP$|\\(REP\\)$", ignore_case = TRUE)) ~ "REPUBLICAN",
          str_detect(candidate, regex("^PGP ", ignore_case = TRUE)) ~ "SOCIALIST",
          str_detect(candidate, regex("^NPA ", ignore_case = TRUE)) ~ "NONPARTISAN",
          str_detect(candidate, regex("^PRO ", ignore_case = TRUE)) ~ "PROGRESSIVE",
          str_detect(candidate, regex("^IND |^IAP ", ignore_case = TRUE)) ~ "INDEPENDENT",
          str_detect(candidate, regex("^GLC ", ignore_case = TRUE)) ~ "GRASSROOTS-LEGALIZE CANNABIS",
          str_detect(candidate, regex("Write", ignore_case = TRUE)) ~ "OTHER",
          str_detect(candidate, "undervote|overvote|No image found") ~ NA_character_,
          is.na(candidate) ~ NA_character_,
          .default = party_detailed
        ),
        candidate = case_when(
          office == "US PRESIDENT" & str_detect(candidate, regex("Biden", ignore_case = TRUE)) ~ "JOSEPH R BIDEN",
          office == "US PRESIDENT" & str_detect(candidate, regex("Trump", ignore_case = TRUE)) ~ "DONALD J TRUMP",
          office == "US PRESIDENT" & str_detect(candidate, regex("Jorgensen", ignore_case = TRUE)) ~ "JO JORGENSEN",
          office == "US PRESIDENT" & str_detect(candidate, regex("Hawkins", ignore_case = TRUE)) ~ "HOWIE HAWKINS",
          office == "US PRESIDENT" & str_detect(candidate, regex("Pierce", ignore_case = TRUE)) ~ "BROCK PIERCE",
          office == "US PRESIDENT" & str_detect(candidate, regex("Blankenship", ignore_case = TRUE)) ~ "DON BLANKENSHIP",
          office == "US PRESIDENT" & str_detect(candidate, regex("Janos", ignore_case = TRUE)) ~ "JAMES G JANOS",
          office == "US PRESIDENT" & str_detect(candidate, regex("Fuente|Rocky", ignore_case = TRUE)) ~ "ROCQUE DE LA FUENTE",
          office == "US PRESIDENT" & str_detect(candidate, regex("Kanye|West", ignore_case = TRUE)) ~ "KANYE WEST",
          office == "US PRESIDENT" & str_detect(candidate, regex("Carroll", ignore_case = TRUE)) ~ "BRIAN T CARROLL",
          office == "US PRESIDENT" & str_detect(candidate, regex("Gloria|Riva", ignore_case = TRUE)) ~ "GLORIA LA RIVA",
          .default = str_remove_all(candidate, regex("^CPF |^GRN |^LBT |^DEM | DEM$|^REP | REP$|^PGP |^NPA |^PRO |^IND|^LIB | LIB$|\\(REP\\)$|\\(DEM\\)$|No image found|^DFL | DFL$|^GLC |^LPN | LPN$|^IAP ", ignore_case = TRUE))
        ),
        party_detailed = case_when(
          office == "US PRESIDENT" & candidate == "JOSEPH R BIDEN" ~ "DEMOCRAT",
          office == "US PRESIDENT" & candidate == "DONALD J TRUMP" ~ "REPUBLICAN",
          office == "US PRESIDENT" & candidate == "JO JORGENSEN" ~ "LIBERTARIAN",
          office == "US PRESIDENT" & candidate == "BROCK PIERCE" ~ "INDEPENDENT",
          office == "US PRESIDENT" & candidate == "DON BLANKENSHIP" ~ "CONSTITUTION",
          office == "US PRESIDENT" & candidate == "ROCQUE DE LA FUENTE" ~ "REFORM",
          office == "US PRESIDENT" & candidate == "HOWIE HAWKINS" ~ "GREEN",
          .default = party_detailed
        ),
        candidate = str_remove_all(candidate, "^$|^NA$|^N/A$|\\([^)]*\\)$|[\\p{Mn}]|[[:punct:]]|^ | $"),
        candidate = stri_trans_nfd(candidate),
        candidate = str_to_upper(candidate),
        jurisdiction_name = county_name
      ) |>
      select(
        any_of(c(
          "state", "county_name", "jurisdiction_name", "cvr_id", "precinct", "office", "district",
          "candidate", "party_detailed", "magnitude", "contest"
        ))
      ) |>
      write_dataset(
        path = "data/pass1/",
        format = "parquet",
        partitioning = c("state", "county_name")
      )

    out <- "data/pass1/state=TEXAS/county_name=DENTON"
    
  } 
  else if (s == "NEW JERSEY" & c == "CUMBERLAND") {
    out <- read_csv("data/raw/New Jersey/Cumberland/cvr.csv") |>
      filter(IsVote) |>
      select(
        cvr_id = CVRNumber,
        contest = Contest,
        candidate = Candidate
      ) |>
      mutate(candidate = str_squish(candidate)) |>
      clean_data(s = s, c = c, type = "xml", contests = contests) |> 
      write_data(s, c)
  }
  else if (s == "FLORIDA" & c == "WALTON"){
    out = fread("data/raw/Florida/Walton/Walton FL CVR.csv", encoding = "UTF-8") |> 
      as_tibble() |> 
      clean_data(s = s, c = c, type = "delim", contests = contests) |> 
      write_data(s, c)
    
  }
  else if (s == "FLORIDA") {
    files <- list.files(str_c("data/raw/Florida/", str_to_title(c)), pattern = "xls", full.names = TRUE)

    out <- map(files, ~ read_excel(.x, .name_repair = "unique_quiet", col_types = "text")) |>
      list_rbind() |>
      clean_data(s, c, type = "delim", contests = contests) |> 
      write_data(s, c)
  } 
  else if (s == "CALIFORNIA" & c == "ALAMEDA") {
    raw <- header_processor(path) |> mutate(cvr_id = 1:n())

    raw1 <- slice_head(raw, prop = 0.5)
    raw2 <- anti_join(raw, raw1, join_by(cvr_id))
    
    clean_data(raw1, s, c, "delim", contests) |> 
      write_dataset(
        path = "data/pass1/",
        format = "parquet",
        basename_template = "part0-{i}.parquet",
        partitioning = c("state", "county_name")
      )
    
    clean_data(raw2, s, c, "delim", contests) |> 
      write_dataset(
        path = "data/pass1/",
        format = "parquet",
        basename_template = "part1-{i}.parquet",
        partitioning = c("state", "county_name")
      )

    # return path
    out <- "data/pass1/state=CALIFORNIA/county_name=ALAMEDA/part-0.parquet"
  } 
  else if (s == "PENNSYLVANIA") {
    files = list.files("data/raw/Pennsylvania/Allegheny", full.names = TRUE)
    lapply(files, fread) |> 
      rbindlist() |> 
      select(cvr_id = cvrNumber, precinct, contest, candidate) |> 
      as_tibble() |> 
      mutate(
        state = s,
        county_name = c
      ) |> 
      left_join(select(contests, -candidate), join_by(state, county_name, contest)) |>
      mutate(
        # clean candidate up a bit at the beginning, so we can catch more party information
        candidate = str_remove_all(candidate, "^$|^NA$|^N/A$|\\([^)]*\\)$|[\\p{Mn}]|[[:punct:]]"),
        candidate = stri_trans_nfd(candidate),
        candidate = str_to_upper(candidate),
        party_detailed = case_when(
          contest == "PROPOSITION" ~ "NONPARTISAN",
          .default = NA_character_
        ),
        candidate = case_when(
          office == "US PRESIDENT" & str_detect(candidate, regex("Biden", ignore_case = TRUE)) ~ "JOSEPH R BIDEN",
          office == "US PRESIDENT" & str_detect(candidate, regex("Trump", ignore_case = TRUE)) ~ "DONALD J TRUMP",
          office == "US PRESIDENT" & str_detect(candidate, regex("Jorgensen", ignore_case = TRUE)) ~ "JO JORGENSEN",
          office == "US PRESIDENT" & str_detect(candidate, regex("Hawkins", ignore_case = TRUE)) ~ "HOWIE HAWKINS",
          office == "US PRESIDENT" & str_detect(candidate, regex("Pierce", ignore_case = TRUE)) ~ "BROCK PIERCE",
          office == "US PRESIDENT" & str_detect(candidate, regex("Blankenship", ignore_case = TRUE)) ~ "DON BLANKENSHIP",
          office == "US PRESIDENT" & str_detect(candidate, regex("Janos", ignore_case = TRUE)) ~ "JAMES G JANOS",
          office == "US PRESIDENT" & str_detect(candidate, regex("Fuente|Rocky", ignore_case = TRUE)) ~ "ROCQUE DE LA FUENTE",
          office == "US PRESIDENT" & str_detect(candidate, regex("Kanye|West", ignore_case = TRUE)) ~ "KANYE WEST",
          office == "US PRESIDENT" & str_detect(candidate, regex("Carroll", ignore_case = TRUE)) ~ "BRIAN T CARROLL",
          office == "US PRESIDENT" & str_detect(candidate, regex("Gloria|Riva", ignore_case = TRUE)) ~ "GLORIA LA RIVA",
          .default = candidate
        ),
        party_detailed = case_when(
          office == "US PRESIDENT" & candidate == "JOSEPH R BIDEN" ~ "DEMOCRAT",
          office == "US PRESIDENT" & candidate == "DONALD J TRUMP" ~ "REPUBLICAN",
          office == "US PRESIDENT" & candidate == "JO JORGENSEN" ~ "LIBERTARIAN",
          office == "US PRESIDENT" & candidate == "BROCK PIERCE" ~ "INDEPENDENT",
          office == "US PRESIDENT" & candidate == "DON BLANKENSHIP" ~ "CONSTITUTION",
          office == "US PRESIDENT" & candidate == "ROCQUE DE LA FUENTE" ~ "REFORM",
          office == "US PRESIDENT" & candidate == "HOWIE HAWKINS" ~ "GREEN",
          .default = party_detailed
        ),
        candidate = str_remove_all(candidate, "^$|^NA$|^N/A$|\\([^)]*\\)$|[\\p{Mn}]|[[:punct:]]"),
        candidate = str_squish(candidate),
        candidate = if_else(str_detect(candidate, "WRITE"), "WRITEIN", candidate),
        jurisdiction_name = county_name
      ) |>
      select(
        # generic function since some columns might be missing in some states
        # for odd reasons. Most likely to be missing is `precinct`
        any_of(c(
          "state", "county_name", "jurisdiction_name", "cvr_id", "precinct", "office", "district",
          "candidate", "party_detailed", "magnitude", "contest"
        ))
      ) |> 
      write_data(s, c)
    
    # return path
    out <- "data/pass1/state=PENNSYLVANIA/county_name=ALLEGHENY/part-0.parquet"
      
    
  }
  else {
    out <- NULL
  }

  return(out)
}

merge_party <- function(party_meta, pass1, state, county_name){
  
  read_parquet(pass1) |> 
    mutate(
      state = state,
      county_name = county_name
    ) |> 
    left_join(party_meta, by = join_by(state, office, district, candidate)) |> 
    mutate(party_detailed = ifelse(is.na(party_detailed.y), party_detailed.x, party_detailed.y)) |> 
    select(-party_detailed.x, -party_detailed.y) |>
    write_dataset("data/pass2", format = "parquet", partitioning = c("state", "county_name"))
  
  str_replace(pass1, fixed("pass1"), fixed("pass2"))
  
}

get_party_meta <- function(path){
  
  read_csv(path) |> 
    filter(is.na(issue) | (!is.na(`fixed error?`))) |> 
    drop_na(candidate_medsl) |> 
    mutate(across(everything(), str_to_upper)) |> 
    select(state:district, candidate = candidate_medsl, party_detailed)
  
}