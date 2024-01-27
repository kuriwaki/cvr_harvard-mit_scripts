* Extract clean long file for the top five offices from Snyder files

#delimit ;

local filelist
AK_Statewide_long.dta
AZ_Maricopa_long.dta
AZ_Pima_long.dta
AZ_Santa_Cruz_long.dta
AZ_Yuma_long.dta
CA_Alameda_long.dta
CA_Amador_long.dta
CA_Contra_Costa_long.dta
CA_Del_Norte_long.dta
CA_El_Dorado_long.dta
CA_Fresno_long.dta
CA_Imperial_long.dta
CA_Inyo_long.dta
CA_Kern_long.dta
CA_Kings_long.dta
CA_Los_Angeles_long.dta
CA_Marin_long.dta
CA_Mariposa_long.dta
CA_Merced_long.dta
CA_Orange_long.dta
CA_Placer_long.dta
CA_Riverside_long.dta
CA_San_Benito_long.dta
CA_San_Bernardino_long.dta
CA_San_Diego_long.dta
CA_San_Francisco_long.dta
CA_San_Luis_Obispo_long.dta
CA_San_Mateo_long.dta
CA_Santa_Barbara_long.dta
CA_Santa_Clara_long.dta
CA_Santa_Cruz_long.dta
CA_Shasta_long.dta
CA_Sonoma_long.dta
CA_Sutter_long.dta
CA_Tehama_long.dta
CA_Tuolumne_long.dta
CA_Ventura_long.dta
CA_Yuba_long.dta
CO_Adams_long.dta
CO_Alamosa_long.dta
CO_Arapahoe_long.dta 
CO_Archuleta_long.dta
CO_Bent_long.dta
CO_Boulder_long.dta
CO_Broomfield_long.dta
CO_Chaffee_long.dta
CO_Cheyenne_long.dta
CO_Clear_Creek_long.dta
CO_Conejos_long.dta
CO_Costilla_long.dta
CO_Crowley_long.dta
CO_Custer_long.dta
CO_Delta_long.dta
CO_Denver_long.dta 
CO_Dolores_long.dta
CO_Douglas_long.dta
CO_Eagle_long.dta
CO_El_Paso_long.dta
CO_Elbert_long.dta
CO_Fremont_long.dta
CO_Garfield_long.dta
CO_Gilpin_long.dta
CO_Grand_long.dta
CO_Gunnison_long.dta
CO_Hinsdale_long.dta
CO_Huerfano_long.dta
CO_Jackson_long.dta
CO_Jefferson_long.dta
CO_Kiowa_long.dta
CO_Kit_Carson_long.dta
CO_La_Plata_long.dta
CO_Lake_long.dta
CO_Larimer_long.dta
CO_Las_Animas_long.dta
CO_Lincoln_long.dta
CO_Logan_long.dta
CO_Mesa_long.dta
CO_Mineral_long.dta
CO_Moffat_long.dta
CO_Montezuma_long.dta
CO_Montrose_long.dta
CO_Morgan_long.dta
CA_Orange_long.dta
CO_Otero_long.dta
CO_Ouray_long.dta
CO_Park_long.dta
CO_Phillips_long.dta
CO_Pitkin_long.dta
CO_Prowers_long.dta
CO_Pueblo_long.dta
CO_Rio_Blanco_long.dta
CO_Rio_Grande_long.dta
CO_Routt_long.dta
CO_Saguache_long.dta
CO_San_Miguel_long.dta
CO_Sedgwick_long.dta
CO_Summit_long.dta
CO_Teller_long.dta
CO_Washington_long.dta
CO_Weld_long.dta
CO_Yuma_long.dta
FL_Bay_long.dta
FL_Bradford_long.dta
FL_Calhoun_long.dta
FL_Citrus_long.dta
FL_Clay_long.dta
FL_Collier_long.dta
FL_Duval_long.dta
FL_Escambia_long.dta
FL_Flagler_long.dta
FL_Gadsden_long.dta
FL_Gulf_long.dta
FL_Hamilton_long.dta
FL_Highlands_long.dta
FL_Hillsborough_long.dta
FL_Holmes_long.dta
FL_Indian_River_long.dta
FL_Jackson_long.dta
FL_Lafayette_long.dta
FL_Lake_long.dta
FL_Lee_long.dta
FL_Manatee_long.dta
FL_Marion_long.dta
FL_Martin_long.dta
FL_Miami-Dade_long.dta
FL_Nassau_long.dta
FL_Okaloosa_long.dta
FL_Orange_long.dta
FL_Palm_long.dta
FL_Pasco_long.dta
FL_Santa_Rosa_long.dta
FL_Sarasota_long.dta
FL_St_Johns_long.dta
FL_Sumter_long.dta
FL_Volusia_long.dta
FL_Wakulla_long.dta
FL_Walton_long.dta
GA_Bacon_long.dta
GA_Barrow_long.dta
GA_Bartow_long.dta
GA_Ben_Hill_long.dta
GA_Berrien_long.dta
GA_Bibb_long.dta
GA_Bleckly_long.dta
GA_Brantley_long.dta
GA_Bryan_long.dta
GA_Burke_long.dta
GA_Camden_long.dta
GA_Candler_long.dta
GA_Carroll_long.dta
GA_Catoosa_long.dta
GA_Charlton_long.dta
GA_Chatham_long.dta
GA_Chatooga_long.dta
GA_Cherokee_long.dta
GA_Clarke_long.dta
GA_Cobb_long.dta
GA_Colquitt_long.dta
GA_Columbia_long.dta
GA_Cook_long.dta
GA_Crisp_long.dta
GA_Dade_long.dta
GA_Dawson_long.dta
GA_DeKalb_long.dta
GA_Douglas_long.dta
GA_Early_long.dta
GA_Echols_long.dta
GA_Elbert_long.dta
GA_Emmanuel_long.dta
GA_Fayette_long.dta
GA_Floyd_long.dta
GA_Forsyth_long.dta
GA_Gilmer_long.dta
GA_Glascock_long.dta
GA_Glynn_long.dta
GA_Gordon_long.dta
GA_Grady_long.dta
GA_Greene_long.dta
GA_Gwinnett_long.dta
GA_Hall_long.dta
GA_Haralson_long.dta
GA_Harris_long.dta
GA_Hart_long.dta
GA_Heard_long.dta
GA_Henry_long.dta
GA_Houston_long.dta
GA_Irwin_long.dta
GA_Jackson_long.dta
GA_Jasper_long.dta
GA_Lanier_long.dta
GA_Laurens_long.dta
GA_Lee_long.dta
GA_Lowndes_long.dta
GA_Lumpkin_long.dta
GA_Madison_long.dta
GA_McDuffie_long.dta
GA_Mitchell_long.dta
GA_Morgan_long.dta
GA_Murray_long.dta
GA_Muscogee_long.dta
GA_Newton_long.dta
GA_Oconee_long.dta
GA_Paulding_long.dta
GA_Pierce_long.dta
GA_Pike_long.dta
GA_Polk_long.dta
GA_Pulaski_long.dta
GA_Putnam_long.dta
GA_Rabun_long.dta
GA_Richmond_long.dta
GA_Rockdale_long.dta
GA_Schley_long.dta
GA_Spalding_long.dta
GA_Talbot_long.dta
GA_Tattnall_long.dta
GA_Terrell_long.dta
GA_Thomas_long.dta
GA_Tift_long.dta
GA_Upson_long.dta
GA_Walker_long.dta
GA_Walton_long.dta
GA_Ware_long.dta
GA_Warren_long.dta
GA_Whitfield_long.dta
GA_Wilcox_long.dta
IL_Bloomington_long.dta
IL_Hamilton_long.dta
IL_Joe_Daviess_long.dta
IL_Lake_long.dta
IL_Monroe_long.dta
IL_Pike_long.dta
IL_Wayne_long.dta
MD_Allegany_long.dta
MD_Baltimore_long.dta
MD_Baltimore_City_long.dta
MD_Calvert_long.dta
MD_Caroline_long.dta
MD_Carroll_long.dta
MD_Cecil_long.dta
MD_Charles_long.dta
MD_Dorchester_long.dta
MD_Frederick_long.dta
MD_Garrett_long.dta
MD_Harford_long.dta
MD_Howard_long.dta
MD_Kent_long.dta
MD_Montgomery_long.dta
MD_Somerset_long.dta
MD_Talbot_long.dta
MD_Washington_long.dta
MD_Wicomico_long.dta
MI_Alger_long.dta
MI_Barry_long.dta
MI_Bay_long.dta
MI_Branch_long.dta
MI_Charlevoix_long.dta
MI_Cheboygan_long.dta
MI_Chippewa_long.dta
MI_Delta_long.dta
MI_Dickinson_long.dta
MI_Emmet_long.dta
MI_Gladwin_long.dta
MI_Iron_long.dta
MI_Isabella_long.dta
MI_Kalamazoo_long.dta
MI_Leelanau_long.dta
MI_Macomb_long.dta
MI_Marquette_long.dta
MI_Mason_long.dta
MI_Missaukee_long.dta
MI_Monroe_long.dta
MI_Montmorency_long.dta
MI_Wayne_long.dta
NJ_Bergen_long.dta
NJ_Camden_long.dta
NJ_Essex_long.dta
NJ_Gloucester_long.dta 
NJ_Hudson_long.dta 
NJ_Middlesex_long.dta 
NJ_Monmouth_long.dta
NJ_Passaic_long.dta
NJ_Salem_long.dta
NJ_Union_long.dta
NM_Chaves_long.dta
NM_Cibola_long.dta
NM_Los_Alamos_long.dta
NM_McKinley_long.dta
NM_Otero_long.dta
NM_Quay_long.dta
NM_Roosevelt_long.dta
NM_Sandoval_long.dta
NM_Sierra_long.dta
NM_Socorro_long.dta
NM_Valencia_long.dta
NV_Carson_City_long.dta
NV_Churchill_long.dta
NV_Douglas_long.dta
NV_Elko_long.dta
NV_Eureka_long.dta
NV_Lander_long.dta
NV_Lincoln_long.dta
NV_Lyon_long.dta
NV_Mineral_long.dta
NV_Nye_long.dta
NV_Pershing_long.dta
NV_Storey_long.dta
NV_Washoe_long.dta
NV_White_Pine_long.dta
OH_Allen_long.dta
OH_Ashtabula_long.dta
OH_Auglaize_long.dta
OH_Belmont_long.dta
OH_Butler_long.dta
OH_Champaign_long.dta
OH_Clark_long.dta
OH_Clermont_long.dta
OH_Clinton_long.dta
OH_Cuyahoga_long.dta
OH_Darke_long.dta
OH_Erie_long.dta
OH_Fayette_long.dta
OH_Franklin_long.dta
OH_Gallia_long.dta
OH_Greene_long.dta
OH_Hancock_long.dta
OH_Highland_long.dta
OH_Hocking_long.dta
OH_Logan_long.dta
OH_Miami_long.dta
OH_Monroe_long.dta
OH_Pickaway_long.dta
OH_Preble_long.dta
OH_Putnam_long.dta
OH_Richland_long.dta
OH_Ross_long.dta
OH_Seneca_long.dta
OH_Shelby_long.dta
OH_Trumbull_long.dta
OH_Tuscarawas_long.dta
OH_Van_Wert_long.dta
OH_Wood_long.dta
OR_Clatsop_long.dta
OR_Columbia_long.dta
OR_Deschutes_long.dta
OR_Douglas_long.dta
OR_Harney_long.dta
OR_Josephine_long.dta
OR_Klamath_long.dta
OR_Lincoln_long.dta
OR_Linn_long.dta
OR_Polk_long.dta
OR_Union_long.dta
OR_Wasco_long.dta
OR_Washington_long.dta
RI_Statewide_long.dta
TN_Williamson_long.dta
/* TN_Wilson_long_2022.dta */
TX_Andrews_long.dta
TX_Bosque_long.dta
TX_Cameron_long.dta
TX_Childress_long.dta
TX_Coleman_long.dta
TX_Collin_long.dta
TX_Cooke_long.dta
TX_Cottle_long.dta
TX_Crane_long.dta
TX_Dallas_long.dta
TX_Ellis_long.dta
TX_Erath_long.dta
TX_Fort_Bend_long.dta
TX_Grayson_long.dta
TX_Guadelupe_long.dta
TX_Hockley_long.dta
TX_Howard_long.dta
TX_Kendall_long.dta
TX_Lee_long.dta
TX_Montague_long.dta
TX_Navarro_long.dta
TX_Oldham_long.dta
TX_Orange_long.dta
TX_Parmer_long.dta
TX_Scurry_long.dta
TX_Smith_long.dta
TX_Stephens_long.dta
TX_Travis_long.dta
TX_Walker_long.dta
TX_Washington_long.dta
TX_Wharton_long.dta
TX_Williamson_long.dta
UT_San_Juan_long.dta
WI_Brown_long.dta
WI_Calumet_long.dta
WI_Dane_long.dta
WI_Eau_Claire_long.dta
WI_Jefferson_long.dta
WI_Kenosha_long.dta
WI_Outagamie_long.dta
WI_Pierce_long.dta
WI_Sauk_long.dta
WI_Sheboygan_long.dta
WI_St_Croix_long.dta
WI_Waukesha_long.dta
WI_Wood_long.dta
WV_Nicholas_long.dta
WV_Wood_long.dta
;

cd "~/Dropbox/CVR_Data_Shared/data_main/";

* Subset to five offices and stack them into one long dataset;
drop _all;
set obs 1;
gen state = "";
save "~/Downloads/snyder_subset_dta", replace;

scalar t1 = c(current_time);
foreach k of local filelist {;
  local state = substr("`k'", 1, 2);
  local county = substr("`k'", 4, .);
  local county = subinstr("`county'", "_long.dta", "", .);
  noisily display "`k'  `state'  `county'";

  use "STATA_long/`k'", clear;
  keep if inlist(item, "US_PRES", "US_REP", "US_SEN", "ST_REP", "ST_SEN");
  gen state = "`state'";
  gen county = "`county'";
  drop if cvr_id == .;

  keep state county cvr_id column item choice_id;

  append using "~/Downloads/snyder_subset_dta";

  save "~/Downloads/snyder_subset_dta", replace;
};

drop if state == "";
encode state, gen(newst);
encode county, gen(newct);
encode item, gen(newitem);
drop state county item;
rename newst state;
rename newct county;
rename newitem item;
order state county cvr_id column item choice_id;
compress;
save "~/Projects/snyder_subset_dta", replace;
scalar t2 = c(current_time);
display (clock(t2, "hms") - clock(t1, "hms")) / (1000*60) " minutes";
// 94 minutes by compressing each time


