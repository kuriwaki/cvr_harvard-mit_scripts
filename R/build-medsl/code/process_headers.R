library(tidyverse)
library(glue)

# Get the command-line arguments
file <- commandArgs(trailingOnly = TRUE)[1]
file_new = str_replace(file, "\\.csv", "_headers.csv")

df <- read_csv(file,
  col_types = cols(.default = "c"),
  n_max = 2,
  skip = 1,
  show_col_types = FALSE
)

colnames(df) = paste(colnames(df), df[1, ], df[2, ], sep = "_") |> 
  str_remove("\\.\\.\\.\\d+") |> 
  str_remove_all("_NA") |> 
  janitor::make_clean_names(case = "title") |> 
  iconv(to = "UTF-8", sub = "")

df <- df[-1:-2, ]

write_csv(df, file_new)
system(glue("tail -n +5 {file} >> {file_new}"))