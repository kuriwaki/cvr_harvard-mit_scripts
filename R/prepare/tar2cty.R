town2cty <- function() {
  tidycensus::census_api_key("a71658d1b3df0f36e32876b12a119dc398dd84b3")
  
  n2a <- state.abb
  names(n2a) <- state.name
  state <- get_decennial(geography = "state",
                         variables = "P1_001N",
                         year = 2020,
                         state = c("CT", "RI", "ME", "VT", "NH", "MA"),
                         sumfile = "pl") |> 
    glimpse() |> 
    transmute(stateFipsCode = GEOID,
              stateName = NAME,
              state_abb = n2a[NAME])
  
   cty <- get_decennial(geography = "county",
                         variables = "P1_001N",
                         year = 2020,
                         state = c("CT", "RI", "ME", "VT", "NH", "MA"),
                         sumfile = "pl")

   town <- get_decennial(geography = "county subdivision",
                          variables = "P1_001N",
                          year = 2020,
                          state = c("CT", "RI", "ME", "VT", "NH", "MA"),
                          sumfile = "pl")
   town |>
    transmute(countyFipsCode  = str_sub(GEOID, 1, 5),
            stateFipsCode = str_sub(GEOID, 1, 2),
            town = map_chr(str_split(NAME, ","),
                             function(z) z[[1]]) |>
                       str_to_upper(),
            county = str_extract(NAME, "(?<=, ).+?(?= County)") |>
                       str_to_upper()
  ) |>
  mutate( town = str_replace(town, " TOWN$", "")) |>
  left_join(state, by = "stateFipsCode") 
} 
