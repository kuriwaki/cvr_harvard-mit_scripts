library(readr)
library(stringr)

# Get the command-line arguments
file <- commandArgs(trailingOnly = TRUE)[1]
file_new = str_replace(file, "\\.csv", "_headers.csv")

df <- read_csv(
  file,
  col_types = cols(.default = "c"),
  skip = 1,
  show_col_types = FALSE
)

bad_rows = which(df[20] == colnames(df)[20] |> str_remove("\\.\\.\\.\\d+")) |> 
  purrr::map(~ (.x - 1):(.x + 2)) |> 
  unlist()

colnames(df) = paste(colnames(df), df[1, ], df[2, ], sep = "_") |> 
  str_remove("\\.\\.\\.\\d+") |> 
  str_remove_all("_NA") |> 
  janitor::make_clean_names(case = "title") |> 
  iconv(to = "UTF-8", sub = "")

df[-c(bad_rows, 1, 2), ] |> 
  write_csv(file_new)