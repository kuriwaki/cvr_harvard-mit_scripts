library(glue)
library(stringr)

files_r = list.files("code", pattern = "\\.R", recursive = TRUE, full.names = TRUE)
files_py = list.files("code", pattern = "\\.py", recursive = TRUE, full.names = TRUE)

gen_string = function(path, type){

  path_tex = str_replace_all(path, "\\_", "\\\\_")

  glue("\\subsection{[path_tex]}\n\\inputminted{[type]}{[path]}", .open = "[", .close = "]")

}

purrr::map_chr(files_r, ~ gen_string(.x, "R")) |> clipr::write_clip()
purrr::map_chr(files_py, ~ gen_string(.x, "python")) |> clipr::write_clip()
