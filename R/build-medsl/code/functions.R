DROP_COLS <- c(
  "Cast Vote Record", "Ballot Style", "RowNumber",
  "BoxID", "BoxPosition", "BallotID", "BallotStyleID",
  "PrecinctStyleName", "ScanComputerName", "Status", "Remade",
  "PrecinctStyleName (Redacted to Protect Voter Privacy)",
  "Cvr Number", "Tabulator Num", "Batch Id", "Record Id",
  "Imprinted Id", "Ballot Type", "Counting Group", "Precinct Portion",
  "Dummy Row Number", "Row Number", "Image Path", "Session Type", "Cast.Vote.Record",
  "Ballot.Style", "Voter Flag", "Modified", "Card Info", "Pdf Name", "Unique Voting Identifier",
  "Voting Session Identifier", "CVRNumber", "BallotTypeId",
  "TabulatorId", "BatchId", "RecordId", "Tabulator Name", "Is Current",
  "Box Id", "Box Position", "Ballot Id", "Ballot Style Id", "Scan Computer Name"
)

RENAME_COLS <- c(
  precinct = "Precinct Portion",
  precinct = "PrecinctPortionID",
  precinct = "Precinct",
  precinct = "PrecinctID",
  precinct = "Precinct Id",
  precinct = "PrecinctId",
  precinct = "Precinct Portion Id",
  precinct = "PrecinctBySplit",
  precinct = "PrecinctPortionId",
  precinct = "Precinct Style Name",
  precinct = "Precinct ID",
  precinct = "precinct_number",
  precinct = "PRECINCT CODE",
  precinct = "PRECINCT NAME"
)

#' Fully clean CVR data
#'
#' @param df A semi-processed data-frame
#' @param s The state
#' @param c The county
#' @param type The type of data-frame
#' @param contests The data-frame of manually categorized contests
#'
#' @return The file path to the written .parquet file
clean_data_write <- function(df, s, c, type, contests) {
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
        county_name = c,
        state = s,
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
          all(candidate.x == "0" | str_detect(candidate.x, regex("undervote", ignore_case=TRUE))),
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
        candidate.x = if_else(voted == 1, candidate.x, "undervote"),
        # this sets us up
        # candidate.x = na_if(candidate.x, "0"),
        candidate = case_when(
          candidate.x == "undervote" ~ "undervote",
          candidate.x == "0" ~ NA_character_,
          .default = coalesce(candidate.y, candidate.x)
        ),
      )
  } 
  else if (type == "json") {
    d <- df |>
      mutate(
        county_name = c,
        state = s
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
        county_name = c,
        state = s
      ) |>
      left_join(contests, join_by(state, county_name, contest)) |>
      # manually define the county and state
      mutate(
        candidate = coalesce(candidate.y, candidate.x)
      )
  }

  path <- sprintf("data/pass1/state=%s/county_name=%s/part-0.parquet", s, ifelse(c == "NA" | is.na(c), "", c)) |>
    str_replace_all(fixed(" "), fixed("%20")) |>
    str_replace_all(fixed("'"), fixed("%27"))

  d |>
    # sometimes there were bad contests we only catch in the making of the lookup table
    # those contests have a blank office type, so we just drop those here
    # also drop all the NAs for candidate (these were the 0s in the delim files)
    drop_na(office, candidate) |>
    # one last sanity check for distinctness
    distinct(cvr_id, office, district, candidate, .keep_all = TRUE) |>
    mutate(
      party_detailed = case_when(
        !is.na(party_detailed) ~ str_to_upper(party_detailed),
        str_detect(candidate, regex("^CPF ", ignore_case = TRUE)) ~ "CONSTITUTION",
        str_detect(candidate, regex("^GRN ", ignore_case = TRUE)) ~ "GREEN",
        str_detect(candidate, regex("^LBT |^LIB | LIB$|^LPN | LPN$", ignore_case = TRUE)) ~ "LIBERTARIAN",
        str_detect(candidate, regex("^DEM | DEM$|\\(DEM\\)$|^DFL | DFL$", ignore_case = TRUE)) ~ "DEMOCRAT",
        str_detect(candidate, regex("^REP | REP$|\\(REP\\)$", ignore_case = TRUE)) ~ "REPUBLICAN",
        str_detect(candidate, regex("^PGP ", ignore_case = TRUE)) ~ "SOCIALIST",
        str_detect(candidate, regex("^NPA ", ignore_case = TRUE)) ~ "NONPARTISAN",
        str_detect(candidate, regex("^PRO ", ignore_case = TRUE)) ~ "PROGRESSIVE",
        str_detect(candidate, regex("^IND |^IAP ", ignore_case = TRUE)) ~ "INDEPENDENT",
        str_detect(candidate, regex("^GLC ", ignore_case = TRUE)) ~ "GRASSROOTS-LEGALIZE CANNABIS",
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
      candidate = stri_trans_nfd(candidate),
      candidate = str_to_upper(candidate),
      candidate = str_squish(candidate),
      # special field for Alaska
      jurisdiction_name = ifelse(s == "ALASKA", str_to_upper(str_extract(precinct, "District \\d+")), c)
    ) |>
    select(
      # generic function since some columns might be missing in some states
      # for odd reasons. Most likely to be missing is `precinct`
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

  # return path
  return(path)
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

  clean_data_write(d, state, county_name, type = "delim", contests = contests)
}

process_json <- function(dir, state = NA, county_name = NA, contests = NULL, contest_only = FALSE) {
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
      mutate(Original = coalesce(Original, Modified)) |>
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
        marks = "Marks"
      ) |>
      unnest_longer(marks) |>
      hoist(
        .col = marks,
        candidate_id = "CandidateId",
        party_id = "PartyId",
        magnitude = "Rank",
        is_vote = "IsVote"
      ) |>
      filter(is_vote) |>
      select(TabulatorId, BatchId, RecordId, precinctportion_id:magnitude)
  }

  files <- list.files(path = dir, pattern = "CvrExport|CVRExport", full.names = TRUE, recursive = TRUE)

  plan(multisession, workers = 8)
  
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

  return(clean_data_write(d, state, county_name, type = "json", contests))
}

process_xml <- function(dir, state = NA, county_name = NA, contests = NULL, contest_only = FALSE) {
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

  plan(multisession, workers = 8)
  
  xmls <- future_imap(files, xml_parser) |> list_rbind()
  
  plan(sequential)

  if (contest_only) {
    out <- distinct(xmls, contest) |>
      pull() |>
      unname()
  } else {
    out <- clean_data_write(xmls, state, county_name, "xml", contests)
  }

  return(out)
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
    
  } else if (s == "NEW JERSEY" & c == "CUMBERLAND") {
    out <- read_csv("data/raw/New Jersey/Cumberland/cvr.csv") |>
      filter(IsVote) |>
      select(
        cvr_id = CVRNumber,
        contest = Contest,
        candidate = Candidate
      ) |>
      mutate(candidate = str_squish(candidate)) |>
      clean_data_write(s = s, c = c, type = "xml", contests = contests)
  } else if (s == "FLORIDA") {
    files <- list.files(str_c("data/raw/Florida/", str_to_title(c)), pattern = "xls", full.names = TRUE)

    out <- map(files, ~ read_excel(.x, .name_repair = "unique_quiet", col_types = "text")) |>
      list_rbind() |>
      clean_data_write(s, c, type = "delim", contests = contests)
  } else if (s == "CALIFORNIA" & c == "ALAMEDA") {
    raw <- header_processor(path) |> mutate(cvr_id = 1:n())

    raw1 <- slice_head(raw, prop = 0.5)
    raw2 <- anti_join(raw, raw1, join_by(cvr_id))

    clean_alameda <- function(df, i) {
      df |>
        rename(any_of(RENAME_COLS)) |>
        select(-any_of(DROP_COLS)) |>
        mutate(
          county_name = c,
          state = s,
          cvr_id = 1:n()
        ) |>
        pivot_longer(
          cols = -any_of(c("state", "county_name", "cvr_id", "precinct")),
          names_to = "contest",
          values_to = "candidate",
          values_drop_na = TRUE
        ) |>
        left_join(contests, join_by(state, county_name, contest)) |>
        mutate(
          voted = ifelse(
            all(candidate.x == "0"),
            0,
            1
          ),
          .by = c("cvr_id", "office", "district")
        ) |>
        mutate(
          candidate.x = if_else(voted == 1, candidate.x, "undervote"),
          candidate = case_when(
            candidate.x == "undervote" ~ "undervote",
            candidate.x == "0" ~ NA_character_,
            .default = coalesce(candidate.y, candidate.x)
          ),
        ) |>
        drop_na(office, candidate) |>
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
          candidate = str_remove_all(candidate, "^$|^NA$|^N/A$|\\([^)]*\\)$|[\\p{Mn}]|[[:punct:]]"),
          candidate = stri_trans_nfd(candidate),
          candidate = str_to_upper(candidate),
          candidate = str_squish(candidate),
          jurisdiction_name = c,
          state = "CALIFORNIA_DUMMY",
          county_name = str_c("ALAMEDA_", i)
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
    }

    clean_alameda(raw1, 1)
    clean_alameda(raw2, 2)

    open_dataset("data/pass1/state=CALIFORNIA_DUMMY/", format = "parquet") |>
      mutate(
        state = "CALIFORNIA",
        county_name = "ALAMEDA"
      ) |>
      write_dataset(
        path = "data/pass1/",
        format = "parquet",
        partitioning = c("state", "county_name")
      )

    unlink("data/pass1/state=CALIFORNIA_DUMMY/", recursive = TRUE, force = TRUE)

    # return path
    out <- sprintf("data/pass1/state=%s/county_name=%s/part-0.parquet", s, c)
  } else {
    out <- NULL
  }

  return(out)
}

# ============================
# Utility Functions
# ============================

# helper to get the raw contest names for manual classification
get_raw_contests <- function(paths) {
  process_files <- function(path, type, county_name, state) {
    message(sprintf("\nGetting Raw Contests in %s, %s using file: %s \n", county_name, state, path))

    if (type == "delim") {
      if (is_header(path)) {
        contests <- header_processor(path, n = 10) |>
          colnames()
      } else {
        contests <- process_delim(path, n = 10) |>
          colnames()
      }
    } else if (type == "json") {
      contests <- process_json(path, contest_only = TRUE)
    } else if (type == "xml") {
      contests <- process_xml(path, contest_only = TRUE)
    } else if (type == "special") {
      contests <- get_special_contests(state, county_name)
    } else {
      contests <- "FAILED TO PARSE TYPE"
    }

    return(contests)
  }

  out <- paths |>
    filter(type != "xml") |>
    mutate(contest = future_pmap(
      list(path, type, county_name, state),
      possibly(process_files, quiet = FALSE)
    )) |>
    unnest(cols = contest) |>
    filter(!(contest %in% c(DROP_COLS, RENAME_COLS)))

  last_dplyr_warnings()
  problems(out)

  out2 <- paths |>
    filter(type == "xml") |>
    mutate(contest = pmap(
      list(path, type, county_name, state),
      possibly(process_files, quiet = FALSE)
    )) |>
    unnest(cols = contest) |>
    filter(!(contest %in% c(DROP_COLS, RENAME_COLS)))

  last_dplyr_warnings()
  problems(out2)

  bind_rows(out, out2) |> select(-path)
}

# helper function to determine if the file is a "header" style file
is_header <- function(path) {
  # get the top-left cell's contents
  if (str_detect(path, "csv$|CSV$")) {
    d <- read_csv(path,
      n_max = 1, col_select = 1, name_repair = "unique_quiet",
      col_names = FALSE, show_col_types = FALSE
    ) |> pull()
  } else if (str_detect(path, "xls$|xlsx$|XLS$|XLSX$")) {
    d <- read_excel(path, range = "A1", col_names = FALSE, .name_repair = "unique_quiet") |> pull()
  }

  if (str_detect(d, regex("2020|General|November", ignore_case = TRUE))) {
    message(sprintf("\nTOP LEFT CELL OF HEADER FILE IS %s \n", d))
    return(TRUE)
  }

  return(FALSE)
}

# helper function to process files with the "header" style
header_processor <- function(path, n = Inf) {
  # read in file, skipping the first row (this contains the file label only)
  df <- read_csv(path,
    col_types = cols(.default = "c"),
    skip = 1,
    n_max = n,
    name_repair = "unique_quiet",
    show_col_types = FALSE
  )

  # force rows 2-4 to be the new column names
  colnames(df) <- paste(colnames(df), df[1, ], df[2, ], sep = "_")
  df <- df[-1:-2, ]

  # then, clean up the column names to something more sensible
  # and return the df
  d <- rename_with(df, ~ make_clean_names(str_remove_all(
    str_remove(
      .x,
      "\\.\\.\\.\\d+"
    ),
    "_NA"
  ), case = "title"))

  colnames(d) <- iconv(colnames(d), to = "UTF-8", sub = "")

  d
}

# helper function to return the contests dataset
get_contests <- function(state, county) {
  read_csv("metadata/contests.csv",
    col_types = cols(.default = col_character()),
    na = c("", "NA", "#N/A")
  ) |>
    mutate(county_name = replace_na(county_name, "")) |>
    filter(state == state, (county_name == county | state == "ALASKA")) |>
    drop_na(contest) |>
    mutate(across(-contest, str_to_upper)) |>
    mutate(
      district = ifelse(office %in% c("STATE HOUSE", "STATE SENATE", "US HOUSE"),
        str_pad(district, width = 3, side = "left", pad = "0"),
        district
      ),
      district = str_replace(district, fixed("COUNTY_NAME"), county_name),
      district = str_replace(district, fixed("STATEWIDE"), state),
      contest = iconv(contest, from = "ascii", to = "UTF-8", sub = "")
    )
}

# get the raw contests from special counties
get_special_contests <- function(state, county) {
  if (state == "CALIFORNIA" & county == "LOS ANGELES") {
    read_csv("data/raw/California/Los Angeles/CandidateCodes.csv",
      show_col_types = FALSE,
      col_names = c("code", "candidate", "contest"), skip = 1, col_select = 3
    ) |>
      pull(contest)
  } else if (state == "FLORIDA") {
    files <- list.files(str_c("data/raw/Florida/", str_to_title(county)), pattern = "xls", full.names = TRUE)

    map(files, ~ read_excel(.x, n_max = 1, .name_repair = "unique_quiet")) |>
      list_rbind() |>
      colnames()
  } else if (state == "NEVADA" & county == "CLARK") {
    read_csv("data/raw/Nevada/Clark/cvr.csv",
      col_select = "Contest",
      show_col_types = FALSE
    ) |>
      distinct(Contest) |>
      pull(Contest)
  } else if (state == "NEW JERSEY" & county == "CUMBERLAND") {
    read_csv("data/raw/New Jersey/Cumberland/cvr.csv",
      show_col_types = FALSE,
      col_select = "Contest"
    ) |>
      distinct(Contest) |>
      pull(Contest)
  } else if (state == "TEXAS" & county == "DENTON") {
    read_csv("data/raw/Texas/Denton/cvr.csv",
      show_col_types = FALSE,
      col_select = "Race"
    ) |>
      distinct(Race) |>
      pull(Race)
  } else if (state == "TEXAS" & county == "MONTGOMERY") {
    read_csv("data/raw/Texas/Montgomery/cvr.csv",
      show_col_types = FALSE,
      col_select = 1,
      col_names = "contest"
    ) |>
      distinct(contest) |>
      pull(contest)
  } else {
    sprintf("UNKNOWN COMBO PASSED, GIVEN COUNTY: %s AND STATE: %s", county, state)
    return("FAILURE")
  }
}
