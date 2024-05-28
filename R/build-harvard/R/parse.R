
# Output locations ------
username <- Sys.info()["user"]

# other users should make a different clause
if (username %in% c("shirokuriwaki", "sk2983")) {
  PATH_projdir <- "~/Dropbox/CVR_Data_Shared/data_main"
  PATH_jbldir <- "~/Dropbox/CVR_Data_Shared/data_main/to-parquet/JBL/"
  PATH_long <- "~/Downloads/stata_init"
  PATH_long2 = "~/Downloads/stata_init/*/*/*.parquet"
  PATH_prec <- "~/Downloads/cvr_prec/"
  PATH_prec_js <- "~/Downloads/cvr_prec_js/"
  PATH_merged <- "~/Downloads/stata_long"
}

#' changes CA_Orange_long.dta to c("CA", "Orange")
parse_js_fname <- function(chr) {
  st <- str_sub(chr, 1, 2)
  ct <- str_extract(chr, "(?<=[A-Z][A-Z]_).*(?=(_cvr_info|_long).dta)")
  c(state = st, county = ct)
}


match_field_name <- function(values, pattern) {
  str_detect(values,
             sprintf("(^|\\|)\\s*%s\\s*(\\||$)",
                     pattern))
}

# See https://www.statology.org/r-add-column-if-does-not-exist/
add_cols <- function(df, cols) {
  add <- cols[!cols %in% names(df)]
  if(length(add) != 0) df[add] <- NA_character_
  return(df)
}

