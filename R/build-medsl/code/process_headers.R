library(tidyverse)

# Get the command-line arguments
args <- commandArgs(trailingOnly = TRUE)

# The first argument is the file path
file <- args[1]

header_processor <- function(file) {
  df <- read_csv(file,
                 col_types = cols(.default = "c"),
                 skip = 1,
                 show_col_types = FALSE
  )
  
  colnames(df) <- paste(colnames(df), df[1, ], df[2, ], sep = "_")
  df <- df[-1:-2, ]
  
  rename_with(df, ~ janitor::make_clean_names(str_remove_all(
    str_remove(
      .x,
      "\\.\\.\\.\\d+"
    ),
    "_NA"
  ), case = "title"))
}

# Call the function with the file path
header_processor(file) |>
    write_csv(str_replace(file, "\\.csv", "_headers.csv"))