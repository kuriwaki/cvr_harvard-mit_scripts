reallocate_wi_prec <- function(tbl) {
  # Patch for misallocated precincts
  tbl |>
    mutate(
      county_name = ifelse(county_name == "BROWN" & precinct == "Village of Pulaski W4, 7", "PULASKI", county_name),
      county_name = ifelse(county_name == "BROWN" & precinct == "Village of Wrightstown W4", "OUTAGAMIE", county_name),
      county_name = ifelse(county_name == "DANE" & precinct %in% c("V Brooklyn Wd 2", "V Belleville Wd 3"), "GREEN", county_name),
      county_name = ifelse(county_name == "DANE" & precinct == "V Cambridge Wd 1", "JEFFERSON", county_name),
      county_name = ifelse(county_name == "PIERCE" & precinct %in% c("Village of Spring Valley, Ward 3", "City of River Falls, Wards 1-4,15"), "ST CROIX", county_name),
      jurisdiction_name = ifelse(state == "WISCONSIN", county_name, jurisdiction_name)
    )
}
