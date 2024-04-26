mit_parq <- open_dataset("data/MEDSL/cvrs_statewide/")
harv_parq <- open_dataset("data/harvard/cvrs_long/")

count(mit_parq, party_detailed) |> collect()
count(harv_parq, item) |> collect()




