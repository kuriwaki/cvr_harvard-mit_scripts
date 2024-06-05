merge_party <- function(pass1, state, county, party_meta){
  
  open_dataset("data/pass1") |> 
    filter(state == state, county_name == county) |> 
    left_join(party_meta, by = join_by(state, office, district, candidate)) |> 
    mutate(party_detailed = coalesce(party_detailed.y, party_detailed.x)) |> 
    select(-party_detailed.x, -party_detailed.y) |>
    write_dataset("data/pass2", format = "parquet", partitioning = c("state", "county_name"))
  
}

get_party_meta <- function(path){
  
  read_csv(path) |> 
    filter(is.na(issue) | (!is.na(`fixed error?`))) |> 
    drop_na(candidate_medsl) |> 
    select(state:district, candidate = candidate_medsl, party_detailed)
  
}