cd "~/Dropbox/CVR_Harvard-MIT"
capture log close
log using "02_merge-party_snyder.log", replace

* long data (N = 170M)
use "~/Projects/snyder_subset_dta.dta", clear
count
local label lab_item: value label item
local label lab_state: value label state
local label lab_county: value label county

* format metadata
preserve
use "~/Dropbox/CVR_Data_Shared/data_main/item_choice_info.dta", clear
* make state county item conform to same label as the individual
encode item, gen(item_lab) label(lab_item)
encode state, gen(state_lab) label(lab_state)
encode county, gen(county_lab) label(lab_county)
drop item state county
rename item_lab item
rename state_lab state
rename county_lab county
keep state county column choice_id item dist choice party incumbent
save tmp_itemchoice_info, replace
restore

* Merge
merge m:1 state county column choice_id using tmp_itemchoice_info
noisily tab _merge
drop _merge
order state county item

save "~/Projects/snyder_subset-with-metadata.dta", replace

log close
