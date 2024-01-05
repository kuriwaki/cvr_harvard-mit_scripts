
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

# Notes

Notes from 2024-01-05 meeting:

- aim to end up with columns district id, contest, candidate (name +
  party). NOT precinct.
- can withhold counties where counts are off by X% or where there’s only
  one vote method
- more challenging: trace back ID row to raw format. full R only script
  for replication
