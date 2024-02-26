#!/usr/bin/env Rscript

# This is a helper script to run the pipeline.

library(tidyverse)
library(targets)

library("tibble")
library("arrow")
library("haven")
library("dplyr")
library("tidyr")
library("purrr")
library("readr")
library("stringr")
library("glue")
library("fs")

targets::tar_make("01_compare-pres-counts/")

