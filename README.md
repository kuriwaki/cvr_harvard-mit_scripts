
<!-- README.md is generated from README.Rmd. Please edit that file -->

# CVR_Harvard-MIT

<!-- badges: start -->
<!-- badges: end -->

The goal of CVR_Harvard-MIT is to coordinate the data combination of the
“Harvard” and “MIT” team’s CVR data.

The goal of `01_compare-pres-counts` is to count presidential candidates
votes between the two teams. `share/viz.html` creates a dashboard to
compare.

The goal of `02_tab-medsl` is for Shiro to tabulate MEDSL’s export and
check it with the Harvard team’s counts.

# Version of Main Dataset

The large CVR datasets are **not** tracked in git. They are instead
stored in each analyst’s local directories.

The **latest version** of MEDSL’s full CVR data is from @mreece13:

``` r
fs::dir_info("data/MEDSL/cvrs_statewide") |> 
  dplyr::select(path, birth_time, modification_time)
#> # A tibble: 26 × 3
#>    path                                  birth_time          modification_time  
#>    <fs::path>                            <dttm>              <dttm>             
#>  1 …ta/MEDSL/cvrs_statewide/state=ALASKA 2024-01-04 15:22:46 2024-01-04 15:22:46
#>  2 …a/MEDSL/cvrs_statewide/state=ARIZONA 2024-01-04 15:22:51 2024-01-04 15:22:51
#>  3 …/MEDSL/cvrs_statewide/state=ARKANSAS 2024-01-04 15:22:47 2024-01-04 15:22:47
#>  4 …EDSL/cvrs_statewide/state=CALIFORNIA 2024-01-04 15:23:02 2024-01-04 15:23:02
#>  5 …/MEDSL/cvrs_statewide/state=COLORADO 2024-01-04 15:23:04 2024-01-04 15:23:04
#>  6 …/MEDSL/cvrs_statewide/state=DELAWARE 2024-01-04 15:23:04 2024-01-04 15:23:04
#>  7 …ewide/state=DISTRICT%20OF%20COLUMBIA 2024-01-04 15:23:04 2024-01-04 15:23:04
#>  8 …a/MEDSL/cvrs_statewide/state=FLORIDA 2024-01-04 15:23:06 2024-01-04 15:23:06
#>  9 …a/MEDSL/cvrs_statewide/state=GEORGIA 2024-01-04 15:23:08 2024-01-04 15:23:08
#> 10 …/MEDSL/cvrs_statewide/state=ILLINOIS 2024-01-04 15:23:08 2024-01-04 15:23:08
#> # ℹ 16 more rows
```

The MD5 hash of the underlying zipfile is

``` bash
md5 data/MEDSL/cvrs_statewide.zip
#> MD5 (data/MEDSL/cvrs_statewide.zip) = 3256317c5a5613dbe188cfd0666338bd
```

# Meeting Notes

Notes from 2024-01-05 meeting:

- aim to end up with columns district id, contest, candidate (name +
  party). NOT precinct.
- can withhold counties where counts are off by X% or where there’s only
  one vote method
- more challenging: trace back ID row to raw format. full R only script
  for replication
