suppressPackageStartupMessages(library(fs))

username <- Sys.info()["user"]
if (username == "shirokuriwaki" | str_detect(username, "^sk[0-9]+")) {
  PATH_parq <- "~/Dropbox/CVR_parquet"
  PATH_medsl_precincts <- "~/Dropbox/MEDSL_Precinct_2020/parquet"
} else if (username %in% c("mason")) {
  PATH_parq <- "~/Dropbox (MIT)/Research/CVR_parquet"
  PATH_medsl_precincts <- "~/Dropbox (MIT)/Research/MEDSL_Precinct_2020/parquet"
}
