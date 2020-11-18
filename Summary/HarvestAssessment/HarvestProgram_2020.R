#================================================================================================
# Harvest Program for 2020
#
# CHANGES FOR 2019:
#    1. Now have biotoxin closures (for Birch Bay) in seasons table. Needed to add
#       code to filter by season_status_code (OR, CR)
#    2. We should no longer write Birch Bay mean_effort_estimates to DB. Use Birch
#       Bay harvest program to generate and write those values.
#
#  BEACH NOTES:
#    1. Effort and Catch errors for Wolfe-Shine are the means of respective values.
#       This is not totally appropriate but workable given the situation.
#    2. The n_obs value for Wolfe-Shine is the max of the two.
#    3. Needed to manually fix BIDN for split Potlatch beaches...again. Use only
#       local shellfish DB for now, then copy to and correct the others from
#       the main local copy.
#
#  QUESTIONS:
#    1.
#
#  ToDo:
# 	1. Create Report markdown...not happening this year.
#   2. Rewrite mil_to_minutes...eliminate need for chron package
#   3. Check if I need Ala Spit in the allowance table for 2019...Its in there
#   4. Send chk_na_species to Melinda later...line 571
#   5. Consider writing all existing biotoxin closure data to DB....or do it
#      spatially????
#
# AS, 2020-11-17
#================================================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(remisc)
library(dplyr)
library(glue)
library(tidyr)
library(DBI)
library(RPostgres)
library(sf)
library(lubridate)
library(chron)
library(tibble)
library(openxlsx)
library(formattable)
library(stringi)

# Keep connections pane from opening
options("connectionObserver" = NULL)
options(dplyr.summarise.inform = FALSE)

# Time the program
strt = Sys.time()

# Set current year variable
current_year = 2020L

# Set season begin and end. Variables needed for winter effort computation
winter_begin = glue("{current_year}-10-01")
winter_end = glue("{current_year}-03-01")

#=======================================================================================
# Define some needed functions. Put in package if useful later ----
#=======================================================================================

#======================================================================================
# Convert obs times (military) to character time (hh:mm:ss).
# Sometimes input is three digits or less
mil_to_hms = function(x) {
  x = as.integer(x)
  mil = case_when(
    x < 1L ~ "0000",
    x > 0L & x < 10L ~ paste0("000", x),
    x > 9L & x < 100L ~ paste0('00', x),
    x > 99 & x < 1000 ~ paste0('0', x),
    TRUE ~ as.character(x))
  hrs = substr(mil, 1, 2)
  mins = substr(mil, 3, 4)
  secs = "00"
  paste0(hrs,':',mins,':',secs)
}

#======================================================================================
# Convert military time to minutes from midnight
mil_to_minutes = function(x) {
  tms = mil_to_hms(x)
  xt = times(tms)
  subhour = chron::hours(xt) * 60L
  submin = chron::minutes(xt)
  subsec = chron::seconds(xt)
  rndsec = if_else(subsec < 30L, 0L, 1L)
  subhour + submin + rndsec
}

#=====================================================================================

# Function to get user for database
pg_user <- function(user_label) {
  Sys.getenv(user_label)
}

# Function to get pw for database
pg_pw <- function(pwd_label) {
  Sys.getenv(pwd_label)
}

# Function to get pw for database
pg_host <- function(host_label) {
  Sys.getenv(host_label)
}

# Function to connect to postgres
pg_con_local = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

# Function to connect to postgres
pg_con_prod = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgres::Postgres(),
    host = pg_host("pg_host_prod"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_prod"),
    port = port)
  con
}

# Function to generate dataframe of tables and row counts in database
db_table_counts = function(db_server = "local", db = "shellfish", schema = "public") {
  if ( db_server == "local" ) {
    db_con = pg_con_local(dbname = db)
  } else {
    db_con = pg_con_prod(dbname = db)
  }
  # Run query
  qry = glue("select table_name FROM information_schema.tables where table_schema = '{schema}'")
  db_tables = DBI::dbGetQuery(db_con, qry) %>%
    pull(table_name)
  tabx = integer(length(db_tables))
  get_count = function(i) {
    tabxi = dbGetQuery(db_con, glue("select count(*) from {schema}.", db_tables[i]))
    as.integer(tabxi$count)
  }
  rc = lapply(seq_along(tabx), get_count)
  dbDisconnect(db_con)
  rcx = as.integer(unlist(rc))
  dtx = tibble(table = db_tables, row_count = rcx)
  dtx = dtx %>%
    arrange(table)
  dtx
}

# Generate a vector of Version 4 UUIDs (RFC 4122)
get_uuid = function(n = 1L) {
  if (!typeof(n) %in% c("double", "integer") ) {
    stop("n must be an integer or double")
  }
  uuid::UUIDgenerate(use.time = FALSE, n = n)
}

#============================================================================================
# Verify the same number of rows exist in both local and production DBs
#============================================================================================

# Get table names and row counts
local_row_counts = db_table_counts(db_server = "local")
prod_row_counts = db_table_counts(db_server = "prod")

# Combine to a dataframe
compare_counts = local_row_counts %>%
  left_join(prod_row_counts, by = "table") %>%
  # Ignore tables that exist in local but not prod
  filter(!table %in% c("geometry_columns", "geometry_columns", "spatial_ref_sys")) %>%
  filter(!substr(table, 1, 10) == "beach_info") %>%
  filter(!substr(table, 1, 12) == "flight_count") %>%
  # Pull out and rename
  select(table, local = row_count.x, prod = row_count.y) %>%
  mutate(row_diff = abs(local - prod))

# Inspect any differences
diff_counts = compare_counts %>%
  filter(!row_diff == 0L)

# Output message
if ( nrow(diff_counts) > 0 ) {
  cat("\nWARNING: Some row counts differ. Inspect 'diff_counts'.\n\n")
} else {
  cat("\nRow counts are the same. Ok to proceed.\n\n")
}

#=======================================================================================
# Get data ----
#=======================================================================================

#===================================================================================
# For bidnfo, pull into three parts, beach with geom, allowance, and tide correction
# 1. beach_st = beach_id, bidn, and geometry...changes occasionally over years.
# 2. beach_tide = beach_id, bidn, beach_name, tide_correction, tide_station.
#    Should be stable over time.
# 3. beach_allow = Allowance, species, status, name, number...changes annually
# 4. Use spatial join to get sfma and mng_region as needed.
#===================================================================================

# 1. Get beach polygons. To use for spatial joins. Needed for flight data and LTCs
#    Only pulling current_year data this time....
qry = glue("select distinct bb.beach_id, bb.beach_number as bidn, bb.beach_name, ",
           "bb.active_datetime, bb.inactive_datetime, bb.geom as geometry ",
           "from beach_boundary_history as bb ",
           "where date_part('year', bb.active_datetime) = {current_year} ",
           "and date_part('year', bb.inactive_datetime) = {current_year}")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Explicitly convert timezones
beach_st = beach_st %>%
  mutate(active_datetime = with_tz(active_datetime, tzone = "America/Los_Angeles")) %>%
  mutate(inactive_datetime = with_tz(inactive_datetime, tzone = "America/Los_Angeles"))

# Check for duplicated beach_ids or BIDNs
chk_dup_beach = beach_st %>%
  filter(duplicated(bidn) | duplicated(beach_id))

# Report if any duplicated beach_ids or BIDNs
if (nrow(chk_dup_beach) > 0) {
  cat("\nWARNING: Duplicated BIDN or beach_id. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated BIDNs or beach_id's. Ok to proceed.\n\n")
}

# 2. Get beach tide data
qry = glue("select distinct b.beach_id, bb.beach_number as bidn, bb.beach_name, ",
           "pl.location_name as tide_station, b.low_tide_correction_minutes as lt_corr ",
           "from beach as b ",
           "inner join beach_boundary_history as bb ",
           "on b.beach_id = bb.beach_id ",
           "left join point_location as pl ",
           "on b.tide_station_location_id = pl.point_location_id ",
           "where date_part('year', bb.active_datetime) = {current_year} ",
           "and date_part('year', bb.inactive_datetime) = {current_year}")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach_tide = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check for duplicated beach_id or BIDNs
chk_dup_beach = beach_tide %>%
  filter(duplicated(bidn) | duplicated(beach_id))

# Report if any duplicated beach_ids or BIDNs
if (nrow(chk_dup_beach) > 0) {
  cat("\nWARNING: Duplicated BIDN or beach_id. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated BIDNs or beach_id's. Ok to proceed.\n\n")
}

#===========================================================
# Allowance data ----
#===========================================================

# 3. Get beach allowance data
qry = glue("select distinct ba.beach_id, bb.beach_number as bidn, b.local_beach_name as beach_name, bs.beach_status_code, ",
           "ee.effort_estimate_type_description as estimate_type, em.egress_model_type_code as egress_model, ",
           "sg.species_group_code, rt.report_type_code as report_type, hu.harvest_unit_type_code as harvest_unit, ",
           "ba.allowance_year, ba.allowable_harvest, ba.comment_text ",
           "from beach_allowance as ba ",
           "inner join beach_boundary_history as bb on ba.beach_id = bb.beach_id ",
           "left join beach as b on ba.beach_id = b.beach_id ",
           "left join beach_status_lut as bs on ba.beach_status_id = bs.beach_status_id ",
           "left join effort_estimate_type_lut as ee on ba.effort_estimate_type_id = ee.effort_estimate_type_id ",
           "left join egress_model_type_lut as em on ba.egress_model_type_id = em.egress_model_type_id ",
           "left join species_group_lut as sg on ba.species_group_id = sg.species_group_id ",
           "left join report_type_lut as rt on ba.report_type_id = rt.report_type_id ",
           "left join harvest_unit_type_lut as hu on ba.harvest_unit_type_id = hu.harvest_unit_type_id ",
           "where ba.allowance_year = {current_year} ",
           "order by b.local_beach_name")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach_allow = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Inspect allowance data for beach_names that should be harmonized
# Update names using "correct_all_beach_names.R"
name_dups = beach_allow %>%
  select(beach_id, bidn, beach_name) %>%
  distinct() %>%
  filter(duplicated(beach_id)) %>%
  select(beach_id) %>%
  left_join(beach_allow, by = "beach_id") %>%
  select(beach_id, bidn, beach_name) %>%
  distinct()

# Report if any names differ in allowance data
if (nrow(name_dups) > 0) {
  cat("\nWARNING: Some beach_names differ. Do not pass go!\n\n")
} else {
  cat("\nNo beach_names differ. Ok to proceed.\n\n")
}

#===========================================================
# Seasons data ----
#===========================================================

# 4. Get beach season data. Get same number or rows, with or without "and end_date" part
qry = glue("select distinct bs.beach_id, bb.beach_number as bidn, bb.beach_name, ",
           "ss.season_status_code, ss.season_status_description, sg.species_group_code, ",
           "bs.season_start_datetime as season_start, bs.season_end_datetime as season_end, ",
           "bs.season_description, bs.comment_text ",
           "from beach_season as bs ",
           "inner join beach_boundary_history as bb on bs.beach_id = bb.beach_id ",
           "left join season_status_lut as ss on bs.season_status_id = ss.season_status_id ",
           "left join species_group_lut as sg on bs.species_group_id = sg.species_group_id ",
           "where date_part('year', bs.season_start_datetime) >= {current_year - 8} ",
           "and date_part('year', bs.season_start_datetime) <= {current_year} ",
           "and ss.season_status_code in ('OR', 'CR') ",
           "order by bb.beach_name, sg.species_group_code, bs.season_start_datetime")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach_season = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
beach_season = beach_season %>%
  mutate(season_start = with_tz(season_start, tzone = "America/Los_Angeles")) %>%
  mutate(season_end = with_tz(season_end, tzone = "America/Los_Angeles"))

# Inspect seasons data for beach_names that should be harmonized
# Update names using "correct_all_beach_names.R"
name_dups = beach_season %>%
  select(beach_id, bidn, beach_name) %>%
  distinct() %>%
  filter(duplicated(beach_id)) %>%
  select(beach_id) %>%
  left_join(beach_season, by = "beach_id") %>%
  select(beach_id, bidn, beach_name) %>%
  distinct()

# Report if any names differ in allowance data
if (nrow(name_dups) > 0) {
  cat("\nWARNING: Some beach_names differ. Do not pass go!\n\n")
} else {
  cat("\nNo beach_names differ. Ok to proceed.\n\n")
}

#===========================================================
# Tide data ----
#===========================================================

# Get tide data
qry = glue("select distinct t.low_tide_datetime as tide_date, pl.location_name as tide_station, ",
           "t.tide_time_minutes as tide_time, t.tide_height_feet as tide_height, ",
           "ts.tide_strata_code as tide_strata ",
           "from tide as t ",
           "left join point_location as pl ",
           "on t.tide_station_location_id = pl.point_location_id ",
           "left join tide_strata_lut as ts ",
           "on t.tide_strata_id = ts.tide_strata_id ",
           "where date_part('year', t.low_tide_datetime) >= {current_year - 8} ",
           "and date_part('year', t.low_tide_datetime) <= {current_year}")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
tide = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones...verified result vs ClamTides
tide = tide %>%
  mutate(tide_date = with_tz(tide_date, tzone = "America/Los_Angeles")) %>%
  mutate(tide_date = format(tide_date))

#===========================================================
# Read shellfish_management_area as geometry data ----
# Get exactly the same when reading with RPostgresql
#===========================================================

# Import from shellfish DB
qry = glue::glue("select shellfish_management_area_id, shellfish_area_code, ",
                 "geom AS geometry ",
                 "from shellfish_management_area_lut")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
sfma_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

#===========================================================
# Read management_region as geometry data ----
#===========================================================

# Import from shellfish DB
qry = glue::glue("select management_region_id, management_region_code, ",
                 "geom AS geometry ",
                 "from management_region_lut")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
mng_reg_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

#===========================================================
# Flight data ----
#===========================================================

# Get flight data. Add bidn later using spatial join
qry = glue("select s.survey_datetime as survey_date, st.survey_type_description as survey_type, ",
           "se.event_datetime as count_time, ht.harvester_type_code as user_type, ",
           "se.harvester_count as uclam, pl.geom as geometry ",
           "from survey as s left join survey_type_lut as st on s.survey_type_id = st.survey_type_id ",
           "left join survey_event as se on s.survey_id = se.survey_id ",
           "left join harvester_type_lut as ht on se.harvester_type_id = ht.harvester_type_id ",
           "left join point_location as pl on se.event_location_id = pl.point_location_id ",
           "where date_part('year', s.survey_datetime) = {current_year} ",
           "and st.survey_type_description in ('Aerial harvester count, clam and oyster', ",
           "'Ground based, low tide harvester count, clam and oyster')")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
flight_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Explicitly convert timezones
flight_st = flight_st %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(count_time = with_tz(count_time, tzone = "America/Los_Angeles"))

# Check hours
sort(unique(substr(format(flight_st$survey_date), 12, 13))) # Should be "". An hour value would indicate incorrect timezone was written to DB
sort(unique(substr(format(flight_st$count_time), 12, 13)))  # Should be range approx 7-18, plus "00"

# Check some other values
unique(substr(flight_st$survey_date, 1, 4))            # Year values
unique(substr(flight_st$count_time, 1, 4))             # Year values
sort(unique(substr(flight_st$count_time, 6, 7)))       # Month values
sort(unique(substr(flight_st$count_time, 12, 13)))     # Hour values
unique(flight_st$survey_type)                          # survey_type
unique(flight_st$user_type)                            # user_type
sort(unique(flight_st$uclam))                          # user_type

#===========================================
# Add beach_id and bidns to flight data ----
#===========================================

# Pull out needed variables from beach_st
bch_st = beach_st %>%
  filter(substr(active_datetime, 1, 4) == current_year) %>%
  select(beach_id, bidn, beach_name, geometry)

# Check for duplicate beach_ids
any(duplicated(bch_st$beach_id))

# # RESULT: None
# chk_beach_id = bch_st %>%
#   filter(duplicated(beach_id))

# Pull out needed variables from flight_st
flt_st = flight_st %>%
  filter(user_type == "rec-clam") %>%
  mutate(survey_type = case_when(
    survey_type == "Aerial harvester count, clam and oyster" ~ "aerial",
    survey_type == "Ground based, low tide harvester count, clam and oyster" ~ "ground")) %>%
  mutate(survey_date = format(survey_date)) %>%
  mutate(count_time = format(count_time)) %>%
  select(survey_date, survey_type, count_time, uclam, geometry)

# # Verify crs
st_crs(bch_st)$epsg
st_crs(flt_st)$epsg

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st)
flt_st = st_join(flt_st, bch_st)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

#======================================================================================
# Split BIDN polygons for 2017 and 2018 have been corrected. Potlatch DNR was deleted
#======================================================================================

# # Warning
# cat("\nWARNING: Need to adjust here for split Potlatch polygons in 2018. I sent note to Roy to correct next year!!!\n\n")
#
# # Correct for separate POTLATCH beach polygons in 2018....THIS YEAR ONLY (I HOPE).
# flt_st = flt_st %>%
#   mutate(beach_id = if_else(beach_id == "769e0b80-049d-40a3-a239-697f1da68194",
#                             "73260974-be95-463b-93e6-07aeb14275c7", beach_id)) %>%
#   mutate(beach_name = if_else(beach_name == "Potlatch-DNR", "Potlatch SP", beach_name)) %>%
#   mutate(bidn = if_else(bidn == 270442L, 270440L, bidn))

#======================================================================================

# Output flight counts with no bidn
# Result:
no_bidn_flt_obs = flt_st %>%
  filter(is.na(bidn)) %>%
  filter(uclam > 0)

# Output flt_obs with missing bidns to shapefile
# Set path for output
#flt_path = glue("C:\\data\\Intertidal\\{current_year}\\EffortData\\FlightCount\\")
#st_write(no_bidn_flt_obs, glue(flt_path, "no_bidn_flt_obs_{current_year}.shp"), delete_layer = TRUE)

# # Manual check in QGIS: Result: All can be deleted
# flt_st = flt_st %>%
#   filter(!is.na(bidn))

# Get list of beach_ids in flight data
flt_bch_ids = unique(flt_st$beach_id)
flt_bch_ids = flt_bch_ids[!is.na(flt_bch_ids)]

# Get list of bidns in flight data
flt_bidns = unique(flt_st$bidn)
flt_bidns = flt_bidns[!is.na(flt_bidns)]

#===========================================================
# Creel data ----
#===========================================================

# Get creel data....need to join bidn by both year and beach_id to avoid duplicate rows
# There are fewer rows than in old program. Due to one row per species. Need to verify means by year.
qry = glue("select se.survey_event_id, s.survey_datetime as survey_date, st.survey_type_description as survey_type, ",
           "s.beach_id, se.event_number, se.event_datetime as creel_time, ",
           "ht.harvester_type_code as harvester_type, se.harvester_count as n_party, ",
           "sp.common_name as species, sh.shell_condition_code as shell_condition, ",
           "spe.species_count, spe.species_weight_gram as species_wt ",
           "from survey as s left join survey_type_lut as st on s.survey_type_id = st.survey_type_id ",
           "left join survey_event as se on s.survey_id = se.survey_id ",
           "left join harvester_type_lut as ht on se.harvester_type_id = ht.harvester_type_id ",
           "left join species_encounter as spe on se.survey_event_id = spe.survey_event_id ",
           "left join species_lut as sp on spe.species_id = sp.species_id ",
           "left join shell_condition_lut as sh on spe.shell_condition_id = sh.shell_condition_id ",
           "where date_part('year', s.survey_datetime) >= {current_year - 8} ",
           "and date_part('year', s.survey_datetime) <= {current_year} ",
           "and st.survey_type_description = 'Creel survey, clam and oyster, catch per unit effort'")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
creel_data = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
creel_data = creel_data %>%
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>%
  mutate(creel_time = with_tz(creel_time, tzone = "America/Los_Angeles"))

# Get bidns and beach_names for creel data. Can't use distinct in creel query otherwise data are lost....
# but joining by survey_event_id works perfect....
qry = glue("select distinct se.survey_event_id, bb.beach_number as bidn, bb.beach_name ",
           "from survey_event as se inner join survey as s on se.survey_id = s.survey_id ",
           "inner join beach_boundary_history as bb on s.beach_id = bb.beach_id ",
           "inner join survey_type_lut as st on s.survey_type_id = st.survey_type_Id ",
           "where date_part('year', s.survey_datetime) >= {current_year - 8} ",
           "and date_part('year', s.survey_datetime) <= {current_year} ",
           "and st.survey_type_description = 'Creel survey, clam and oyster, catch per unit effort'")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
creel_beaches = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check for dups...If dups present...may need to change beach_names in pgAdmin....Then save.
# If dups are present for any beach it will generate multiple extra rows and overestimate harvest
any(duplicated(creel_beaches$survey_event_id))
chk_dup_creel_id = creel_beaches %>%
  filter(duplicated(survey_event_id)) %>%
  select(survey_event_id) %>%
  left_join(creel_beaches, by = "survey_event_id")

# Combine creels with creel_beaches
creel = creel_data %>%
  left_join(creel_beaches, by = "survey_event_id") %>%
  select(beach_id, survey_event_id, bidn, beach_name, survey_date,
         survey_type, event_number, creel_time, harvester_type, n_party,
         species, shell_condition, species_count, species_wt)

# Warning if join adds rows
if (!nrow(creel_data) == nrow(creel)) {
  cat("\nWARNING: Join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Check potlatch creels
sort(unique(creel$beach_name))   # Only Potlatch SP has creels
chk_potlatch_creels = creel %>%
  filter(beach_name == "Potlatch SP") %>%
  arrange(survey_date, event_number)

# Check potlatch creels
sort(unique(creel$beach_name))   # Only Dose SP has creels
sort(unique(creel$bidn))         # Only Dose 270200 has creels
chk_dose_creels = creel %>%
  filter(beach_name == "Dosewallips SP All") %>%
  arrange(survey_date, event_number)

# Verify BIDNs
unique(chk_potlatch_creels$bidn)
unique(chk_dose_creels$bidn)

#===========================================================
# Egress data
#===========================================================

# Get the egress values
qry = glue("select ev.egress_model_name, em.egress_model_interval as egress_interval, ",
           "em.egress_model_ratio as model_ratio, em.egress_model_variance as model_variance ",
           "from egress_model_version as ev ",
           "inner join egress_model as em on ev.egress_model_version_id = em.egress_model_version_id ",
           "where ev.inactive_indicator = false")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
egress = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

#=======================================================================================
# CPUE SECTION ----
#=======================================================================================

#====================================================
# Format Creel data ----
#====================================================

# Inspect some values
unique(creel$survey_type)
unique(creel$harvester_type)    # Should all be rec-clam
unique(creel$species)
unique(creel$shell_condition)   # Both "na" and NA will be displayed. Both are correct.

# Check NA values for species....29 cases. 5 in 2019, otherwise 2016 or earlier
chk_na_species = creel %>%
  filter(is.na(species))

# Check NA values for shell_condition....No worries, all were oysters
chk_na_condition = creel %>%
  filter(is.na(shell_condition) | shell_condition == "na") %>%
  filter(!species == "Pacific oyster")

# Verify all creel beach_ids match to flt data -------------------------

# Check for creel beaches that are not in the flt file
# RESULT: All beaches in creel are also in flt_st
no_flt_crl_one = creel %>%
  filter(!beach_id %in% flt_bch_ids) %>%
  select(beach_id, bidn, beach_name, survey_date, harvester_type) %>%
  distinct() %>%
  arrange(beach_name)

no_flt_crl_two = creel %>%
  filter(!bidn %in% flt_bidns) %>%
  select(beach_id, bidn, beach_name, survey_date, harvester_type) %>%
  distinct() %>%
  arrange(beach_name)

# Warning if any beaches need to be reassigned
if (nrow(no_flt_crl_one) > 0 | nrow(no_flt_crl_two) > 0 ) {
  cat("\nWARNING: Some creel data will not match flight data. Do not pass go!!!\n\n")
} else {
  cat("\nAll Creel data matches beach_ids and bidns in flight data. Ok to proceed.\n\n")
}

# Examine Dose creels
chk_dose = creel %>%
  filter(bidn %in% c(270201L, 270200L)) %>%
  arrange(desc(bidn), survey_date)
unique(chk_dose$bidn)

# Examine Quil creels
chk_quil = creel %>%
  filter(bidn %in% c(270500L, 270900L)) %>%
  arrange(desc(bidn), survey_date)
unique(chk_quil$bidn)

# Examine Quil creels
chk_pot = creel %>%
  filter(bidn %in% c(270442L, 270440L)) %>%
  arrange(desc(bidn), survey_date)
unique(chk_pot$bidn)

# Check all bidns....Only
sort(unique(creel$bidn))

# Step 1. Manually inspect FlightBIDN layer to see if beach is in layer
# Manually inspect flt_st to identify correct beach_id and bidn for all cases in
# no_flt_crl_one and no_flt_crl_two. Then update both beach_id and bidn values as needed.
# Need to set beach_id to match flight file. Creel data are non-spatial and span years.
# Flight data sets the beach_id and bidn value for the year and are spatially joined.
# creel = creel %>%
#   # Dosewallips SP App...BIDN is 270200 in 2017 BIDN polygons
#   mutate(bidn = if_else(bidn == 270201L, 270200L, bidn)) %>%
#   mutate(beach_id = if_else(beach_id == "09CDC9D8-4741-46E4-810D-4F80468FBD48",
#                             stri_trans_toupper("d65072f2-21a5-4d5c-9c17-aa6c0e65ce8c"), beach_id)) %>%
#   # Quilcene Bay Tidelands...BIDN is 270900 in 2017 BIDN polygons
#   mutate(bidn = if_else(bidn == 270500L, 270900L, bidn)) %>%
#   mutate(beach_id = if_else(beach_id == "D6237291-F4FB-40D7-A5D4-0DBE2C6688F5",
#                             stri_trans_toupper("f0439944-d0e9-4c9f-9888-bf2db6b4a4e7"), beach_id)) %>%
#   # No Winas-Maylor Pt E in current year BIDN polygons....so dump
#   filter(!bidn == 240520L)
#
# # Check again for creel beaches that are not in the flt file
# # RESULT: Now all beaches in creel are also in flt_st
# no_flt_crl_one = creel %>%
#   filter(!beach_id %in% flt_bch_ids) %>%
#   select(beach_id, bidn, beach_name, survey_date, harvester_type) %>%
#   distinct() %>%
#   arrange(beach_name)
#
# no_flt_crl_two = creel %>%
#   filter(!bidn %in% flt_bidns) %>%
#   select(beach_id, bidn, beach_name, survey_date, harvester_type) %>%
#   distinct() %>%
#   arrange(beach_name)
#
# # Warning if any beaches need to be reassigned
# if (nrow(no_flt_crl_one) > 0 | nrow(no_flt_crl_two) > 0 ) {
#   cat("\nWARNING: Some creel data will not match flight data. Do not pass go!!!\n\n")
# } else {
#   cat("\nAll Creel data matches beach_ids and bidns in flight data. Ok to proceed.\n\n")
# }

# End beach_id check section -----------------------------------------------------

# Get counts
table(creel$species, useNA = "ifany")
table(creel$shell_condition, useNA = "ifany")

# Format creel data...We don't care about separate varnish CPUE. Maybe in the future.
# For now, lump all other species as oth
crls = creel %>%
  filter(harvester_type == "rec-clam") %>%
  filter(survey_type == "Creel survey, clam and oyster, catch per unit effort") %>%
  mutate(survey_date = format(survey_date)) %>%
  mutate(creel_time = format(creel_time)) %>%
  mutate(species = case_when(
    species == "Pacific oyster" ~ "oys",
    species == "Eastern soft-shell" ~ "ess",
    species == "Manila clam" ~ "man",
    species == "Native littleneck" ~ "nat",
    species == "Horse clam" ~ "hor",
    species == "Geoduck" ~ "geo",
    species == "Cockle" ~ "coc",
    species == "Butter clam" ~ "but",
    is.na(species) ~ "oth")) %>%
  mutate(shell_condition = case_when(
    shell_condition == "broken clam" ~ "broken",
    shell_condition == "unbroken clam" ~ "intact",
    shell_condition == "na" ~ "oyster",
    is.na(shell_condition) ~ "other")) %>%
  select(survey_date, beach_id, bidn, beach_name, event_number,
         creel_time, n_party, species, shell_condition, species_count,
         species_wt)

# Get counts
table(crls$species, useNA = "ifany")
table(crls$shell_condition, useNA = "ifany")

# Verify no future dates
max(unique(as.integer(substr(crls$survey_date, 1, 4))))
max(unique(as.integer(substr(crls$creel_time, 1, 4))))

# Check bidns
sort(unique(crls$bidn))
sort(unique(crls$beach_name))

# Look for crls without a bidn.....None missing
chk_creel = crls %>%
  filter(is.na(bidn))

# Organize columns
crls = crls %>%
  mutate(creel_time = substr(creel_time, 12, 16)) %>%
  mutate(creel_time = as.integer(gsub(":", "", creel_time))) %>%
  mutate(year = as.integer(substr(survey_date, 1, 4))) %>%
  select(bidn, beach_name, year, survey_date, event_number, creel_time,
         n_party, species, shell_condition, species_count, species_wt) %>%
  arrange(bidn, as.Date(survey_date), creel_time)

#=====================================================
# Format Seasons data ----
#====================================================

# Inspect some values
unique(beach_season$season_status_code)
unique(beach_season$species_group_code)
sort(unique(substr(beach_season$season_start, 1, 4)))

# Check for season beaches that are not in the flt file
# RESULT: Dose 270200 for earlier years needs to be updated to 270201
# Dungeness, Mystery Bay, and Spencer Spit can be filtered out
no_flt_seas = beach_season %>%
  filter(!beach_id %in% flt_bch_ids) %>%
  select(beach_id, bidn, beach_name) %>%
  distinct()

# Warning if any beaches need to be reassigned
if (nrow(no_flt_seas) > 0 ) {
  cat("\nWARNING: Some seasons data will not match flight data. Do not pass go!!!\n\n")
} else {
  cat("\nAll Creel seasons matches beach_ids and bidns in flight data. Ok to proceed.\n\n")
}

# Examine Dose seasons...Open all year 2015 through 2017
chk_dose = beach_season %>%
  filter(bidn %in% c(270201L, 270200L)) %>%
  arrange(desc(bidn), season_start)
unique(chk_dose$bidn)

# Examine Quil seasons
chk_quil = beach_season %>%
  filter(bidn %in% c(270500L, 270900L)) %>%
  arrange(desc(bidn), season_start)
unique(chk_quil$bidn)

# Examine Potlatch seasons
chk_pot = beach_season %>%
  filter(bidn %in% c(270442L, 270440L)) %>%
  arrange(desc(bidn), season_start)
unique(chk_pot$bidn)

# Inspect remaining bidns
sort(unique(beach_season$bidn))

# Make the needed adjustments...Otherwise steps below will filter out Dosewallips seasons
# THIS IS ESSENTIAL !!!!!!!!
beach_season = beach_season %>%
  # Dosewallips SP App...BIDN is 270200 in 2020 BIDN polygons...no other dose bidn
  mutate(bidn = if_else(bidn == 270201L, 270200L, bidn)) %>%
  mutate(beach_id = if_else(beach_id == "09cdc9d8-4741-46e4-810d-4f80468fbd48",
                            "d65072f2-21a5-4d5c-9c17-aa6c0e65ce8c", beach_id)) %>%
  # Get rid of beaches where no catch estimates is needed. Get rid of Potlatch DNR to avoid two sets of
  # seasons for the same beach.
  filter(!bidn %in% c(250014L, 250300L, 270442L, 220050L))

# Check again after adjustments
no_flt_seas = beach_season %>%
  filter(!beach_id %in% flt_bch_ids) %>%
  select(beach_id, bidn, beach_name) %>%
  distinct()

# Check again after adjustments
if (nrow(no_flt_seas) > 0 ) {
  cat("\nWARNING: Some seasons data will not match flight data. Do not pass go!!!\n\n")
} else {
  cat("\nAll Creel seasons matches beach_ids and bidns in flight data. Ok to proceed.\n\n")
}

# Check
# sort(unique(flt_bidns))
# flt_bidns = unique(c(flt_bidns, c("270200")))

# Pull out seasons data
seas = beach_season %>%
  filter(bidn %in% flt_bidns) %>%
  mutate(season_start = format(season_start)) %>%
  mutate(season_end = format(season_end))

# Make sure there are nine years of data for every beach
yrs = tibble(year = seq(current_year - 8, current_year))

# Get bidns and beach_names
bidns = tibble(bidn = flt_st$bidn,
               beach_id = flt_st$beach_id,
               beach_name = flt_st$beach_name)

# Get just the bidns
bidns = bidns %>%
  select(bidn, beach_id, beach_name) %>%
  filter(!is.na(bidn)) %>%
  distinct()

# Join to years...need to create full expanded grid to enable computing stats
yrs = yrs %>%
  tidyr::expand(yrs, bidns) %>%
  arrange(beach_name, year)

# Create clam and oyster categories
sp_group = tibble(species_group_code = c("Clam", "Oyster"))

# Join to years
yrs = yrs %>%
  tidyr::expand(yrs, sp_group) %>%
  arrange(beach_name, year, species_group_code)

# Add year value to seas
seas = seas %>%
  mutate(year = as.integer(substr(season_start, 1, 4))) %>%
  arrange(beach_name, year, season_start) %>%
  select(-c(beach_name, beach_id, season_status_description))

# Join to seasons
seas = yrs %>%
  full_join(seas, by = c("bidn", "year", "species_group_code")) %>%
  arrange(beach_name, year, season_start, species_group_code) %>%
  mutate(season_status_code = if_else(is.na(season_status_code), "OR", season_status_code)) %>%
  mutate(season_start = if_else(is.na(season_start), paste0(year, "-01-01"), season_start)) %>%
  mutate(season_end = if_else(is.na(season_end), paste0(year, "-12-31"), season_end))

# Reformat in same shape as the old seas dataset...one line per crlsea
clam_sea = seas %>%
  filter(species_group_code == "Clam") %>%
  select(bidn, beach_id, beach_name, year, begin = season_start,
         end = season_end, clam_code = season_status_code)

oys_sea = seas %>%
  filter(species_group_code == "Oyster") %>%
  select(bidn, beach_id, beach_name, year, begin = season_start,
         end = season_end, oys_code = season_status_code)

# Combine
both_seas = clam_sea %>%
  full_join(oys_sea, by = c("bidn", "year", "beach_id",
                            "beach_name", "begin", "end")) %>%
  mutate(crlsea = paste0(clam_code, oys_code)) %>%
  mutate(crlsea = gsub("R", "", crlsea)) %>%
  mutate(crlsea = trimws(crlsea)) %>%
  select(bidn, beach_id, beach_name, year, begin, end, crlsea) %>%
  arrange(bidn, year, begin) %>%
  distinct()

# Add both_seas to crls. Need to calculate all statistics by crlsea
crl_sea = crls %>%
  select(-beach_name) %>%
  full_join(both_seas, by = c("bidn", "year")) %>%
  mutate(ok = if_else(as.Date(survey_date) >= as.Date(begin) &
                        as.Date(survey_date) <= as.Date(end), "in", "out")) %>%
  filter(ok == "in") %>%
  select(bidn, beach_id, beach_name, year, survey_date, event_number, creel_time,
         n_party, species, species_count, species_wt, shell_condition, crlsea) %>%
  arrange(bidn, survey_date, creel_time)

# Calculate the number of harvesters in each creel
sdate = crl_sea %>%
  select(bidn, year, survey_date, creel_time, n_party, crlsea) %>%
  distinct() %>%
  group_by(bidn, year, crlsea, survey_date) %>%
  mutate(n_harv = sum(n_party, na.rm = TRUE)) %>%
  ungroup() %>%
  select(bidn, year, survey_date, crlsea, n_harv) %>%
  distinct()

# Calculate the number of separate creel-days each year in each Seasons category
nyr = sdate %>%
  group_by(bidn, year, crlsea) %>%
  mutate(n_crls = n()) %>%
  ungroup() %>%
  select(bidn, year, crlsea, n_crls) %>%
  distinct()

# Separate out CO creels, keep all
crls_co = nyr %>%
  filter(crlsea == "CO")

# Separate out OC creels, keep all
crls_oc = nyr %>%
  filter(crlsea == "OC")

# Separate out OO creels. Will keep only most recent 3 years below
nyr_oo = nyr %>%
  filter(crlsea == "OO")

# Select only the most recent three years of data
crls_oo = nyr_oo %>%
  arrange(bidn, desc(year)) %>%
  group_by(bidn) %>%
  mutate(n_seq = row_number(bidn)) %>%
  ungroup() %>%
  filter(n_seq <= 3) %>%
  select(-n_seq)

# Combine creel season data into one dataset with all useable creels
crl_yrs = rbind(crls_co, crls_oc, crls_oo)

# Combine n_harv and n_crls data. Provides picture of creel survey status for last 6 years
all_crls = sdate %>%
  left_join(nyr, by = c("bidn", "year", "crlsea")) %>%
  left_join(bidns, by = "bidn") %>%
  select(bidn, beach_name, year, survey_date, n_harv, n_crls) %>%
  arrange(bidn, year, as.Date(survey_date))

# Combine crlyrs and csea. Gets rid of creel data that's too old
# This contains actual species data for further computation
cr = crl_sea %>%
  inner_join(crl_yrs, by = c("bidn", "year", "crlsea"))

# Join allcrls with crlyrs to trim summarized data down to only creels that will be used
creels = all_crls %>%
  inner_join(crl_yrs, by = c("bidn", "year", "n_crls")) %>%
  select(bidn, beach_name, survey_date, crlsea, n_harv)

# Calculate total numbers of creels for all years by BIDN and crlseason
creel_n = creels %>%
  group_by(bidn, crlsea) %>%
  summarize(n_crls = n()) %>%
  ungroup()

# Combine with creels to get dataset providing creel status summary
creel_status = creels %>%
  left_join(creel_n, by = c("bidn", "crlsea"))

# Get rid of some unneeded datasets
rm(list = c("nyr_oo", "crls_co", "crls_oo", "crls_oc", "all_crls", "creels",
            "creel_n", "crl_yrs", "crl_sea", "nyr", "sdate"))

# CALCULATE WEIGHT FOR BROKEN CLAMS =============================================================
# Creel Flag codes:
#    B=Broken, C=Crab only, G=Geoduck only, M=Mussels only, O=Ok,
#    W=No weight, S=Seaweed only
#    'O' Flags were entered in some of the 2001 entries, remaining 'Ok' entries are 'NA'
#    This is because the newer Access database form only reports codes other than 'O'
#    Program steps below change 'NA' entries to 'O' for consistency and code convenience
#    'W' is normally entered when scale malfunctions or there is too much wind ect.
#    There are also 'W' entries in past files where some species are weighed while others are not
#    Consequently, 'W' entries are treated as broken in the program steps below
# Subset to omit 'C' flag observations (for crabber)
#    There were only two of these in past 6 years (1 in 2006, 1 in 2001)
#    Crabbers are not included in counts by aerial surveyors
#    Crabbers can be effectively spotted from the air
#    The other flag designations are more problematic and need to be included
# Substitutions for NAs in broken clam wts:
#    If there are no clams of same species as broken in the creel substitutions are used
#    The following steps are used to calculate mean weights of broken clams for substitutions
#    1, Calculate mean wt of same species as broken in the same creel
#    2, Calculate mean wt by BIDN and Date
#    3, Calculate mean wt by BIDN, most recent 3 years, or 6 years in the case of CO creels
#    4, Calculate mean wt by all BIDNs, most recent 3 years, or 6 years in the case of CO creels
#    5, If still no value, it's a virtually impossible outlier, so enter a zero
#================================================================================================

# Generate mean weight values by species from each creel
cr = cr %>%
  mutate(mean_wt = species_wt / species_count)

#====================================================
# Calculate substitution values for broken clams ----
#====================================================

# Generate mean weight substitution values within the same creel event...by bidn, date, creel_time, and species

# Generate mean weights by bidn, survey_date, and species.
# Retain file for later use to show species occurrance
sub_one = cr %>%
  group_by(bidn, survey_date, creel_time, species) %>%
  summarize(mean_wt_one = mean(mean_wt, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(mean_wt_one = if_else(is.nan(mean_wt_one), NA_real_, mean_wt_one))

sub_two = cr %>%
  group_by(bidn, survey_date, species) %>%
  summarize(mean_wt_two = mean(mean_wt, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(mean_wt_two = if_else(is.nan(mean_wt_two), NA_real_, mean_wt_two))

# Generate mean weights for each bidn and species over all data, BIDNs pooled
sub_three = cr %>%
  group_by(bidn, species) %>%
  summarize(mean_wt_three = mean(mean_wt, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(mean_wt_three = if_else(is.nan(mean_wt_three), NA_real_, mean_wt_three))

# Generate mean weights for each bidn and species over all data, BIDNs pooled
sub_four = cr %>%
  group_by(species) %>%
  summarize(mean_wt_four = mean(mean_wt, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(mean_wt_four = if_else(is.nan(mean_wt_four), NA_real_, mean_wt_four))

# Combine all substitution values into one file
subs = sub_one %>%
  left_join(sub_two, by = c("bidn", "survey_date", "species")) %>%
  left_join(sub_three, by = c("bidn", "species")) %>%
  left_join(sub_four, by = c("species"))

# # Just check the birch values
# sub_birch = subs %>%
#   filter(bidn == 200060 & species == "but")

# Verify there are no missing sub_three values except for oysters
chk_sub = subs %>%
  filter(is.na(mean_wt_four)) %>%
  filter(!species %in% c("oys", "oth"))

if (nrow(chk_sub) > 0) {
  cat("\nWARNING: Not all cases have a substitution value. Do not pass go!\n\n")
}

# Add substitutions to creel file
cr = cr %>%
  left_join(subs, by = c("bidn", "survey_date", "creel_time", "species"))

# Get rid of some unneeded files
rm(list=c("sub_one", "sub_two", "sub_three", "sub_four", "subs"))

#====================================================
# Calculate mean_wts for broken clams ----
#====================================================

# Calculate mean weights for broken clams
cr = cr %>%
  mutate(broken_mean_wt = case_when(
    shell_condition == "broken" & !is.na(mean_wt_one) ~ mean_wt_one,
    shell_condition == "broken" & is.na(mean_wt_one) & !is.na(mean_wt_two) ~ mean_wt_two,
    shell_condition == "broken" & is.na(mean_wt_one) & is.na(mean_wt_two) &
      !is.na(mean_wt_three) ~ mean_wt_three,
    shell_condition == "broken" & is.na(mean_wt_one) & is.na(mean_wt_two) &
      is.na(mean_wt_three & !is.na(mean_wt_four)) ~ mean_wt_four,
    shell_condition == "broken" & is.na(mean_wt_one) & is.na(mean_wt_two) &
      is.na(mean_wt_three & is.na(mean_wt_four)) ~ 0.0,
    shell_condition == "intact" ~ 0.0
  ))

# Calculate weights for broken clams
cr = cr %>%
  mutate(broken_wt = case_when(
    shell_condition == "broken" ~ species_count * broken_mean_wt,
    shell_condition == "intact" ~ 0.0
  ))

# Calculate weights for clams, broken or intact...put into one column
cr = cr %>%
  mutate(clam_wt = case_when(
    shell_condition == "intact" ~ species_wt,
    shell_condition == "broken" ~ broken_wt,
    species == "oys" ~ 0.0
  )) %>%
  select(bidn, beach_id, beach_name, year, survey_date, crlsea,
         creel_time, n_party, species, species_count, species_wt,
         broken_mean_wt, shell_condition, clam_wt)

# Differences in mean wts between old program an new is that the old one
# calculated a mean wt for each line in the interview to use in substitutions.
# The new program uses a mean of the mean values for each line to use as
# substitution values. This is probably better. Each interview is a unit,
# not each bucket. Also, even for one person, broken clams and overharvest
# may be broken out on more than one line.

#============================================================================
# CALCULATE CPUE VALUES ----

# Step 1: For each day, calculate sum clam wt, sum oys number, sum harvesters
# Step 2: Calculate CPUE for each day (wt/harvester)
# Step 3: Calculate mean and variance for season long CPUE (all years pooled)
#         by BIDN and flt season
#============================================================================

# Get all values aggregated to single creel_time records
cr_time = cr %>%
  group_by(bidn, survey_date, crlsea, creel_time, n_party, species) %>%
  summarize(sum_clam_wt = sum(clam_wt, na.rm = TRUE),
            sum_oys = sum(species_count)) %>%
  mutate(sum_oys = if_else(species == "oys", sum_oys, 0L)) %>%
  ungroup()

# Calculate sum harvesters for each day
cp_harv = cr_time %>%
  select(bidn, survey_date, creel_time, n_party, crlsea) %>%
  distinct() %>%
  group_by(bidn, survey_date, crlsea) %>%
  summarize(sum_party = sum(n_party, na.rm = TRUE)) %>%
  ungroup()

# Calculate sum wt clams for each day
cp_clam = cr_time %>%
  group_by(bidn, survey_date, species, crlsea) %>%
  summarize(sum_clam = sum(sum_clam_wt, na.rm = TRUE),
            sum_oys = sum(sum_oys, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(sum_oys = as.numeric(sum_oys)) %>%
  mutate(sum_species = if_else(species == "oys", sum_oys, sum_clam)) %>%
  select(bidn, survey_date, species, crlsea, sum_species)

# Combine into one dataset
cp = cp_clam %>%
  left_join(cp_harv, by = c("bidn", "survey_date", "crlsea"))

# Calculate CPUE in pounds for each day
cp = cp %>%
  mutate(cpue_day_species = sum_species / sum_party / 453.59237) %>%
  mutate(cpue_day_species = if_else(species == "oys",
                                    sum_species / sum_party, cpue_day_species))

# Add a full set of species to each day to allow calculating mean_cpue
# First get survey days for each bidn. Use crls dataset
bidn_day = creel_status %>%
  select(bidn, survey_date, crlsea) %>%
  distinct()

# Create dataset with full set of species
all_sp = tibble(species = c("man", "nat", "but", "hor", "coc", "ess", "geo", "oys"))

# Expand bidn to include all species
bidn_sp = bidn_day %>%
  tidyr::expand(bidn_day, all_sp) %>%
  arrange(bidn, survey_date, crlsea, species)

# Join expanded bidn_sp with cp to get all categories
cp_all = bidn_sp %>%
  left_join(cp, by = c("bidn", "survey_date", "crlsea", "species"))

# Define fltsea in cp so that creel data can be merged with flight data
cp_all = cp_all %>%
  mutate(fltsea = case_when(
    crlsea == "OO" ~ "OO",
    crlsea == "CO" ~ "CO",
    crlsea == "OC" ~ "OO",
    crlsea == "CC" ~ "OO",
    !crlsea %in% c("OO", "CO", "OC", "CC") ~ "OO"
  ))

# Fill in missing cpue_day_species values with zeros
cp_all = cp_all %>%
  mutate(cpue_day_species = if_else(is.na(cpue_day_species), 0.0, cpue_day_species))

# # Check dose creels
# cp_dose = cp_all %>%
#   filter(bidn == 270200L & fltsea == "OO" & species == "man")
# write.csv(cp_dose, "Dosewallips_cpue_data.csv", row.names = FALSE)

# Calculate the mean of CPUE by bidn and fltsea
cp_mean = cp_all %>%
  group_by(bidn, fltsea, species) %>%
  summarize(mean_cpue = mean(cpue_day_species, na.rm = TRUE)) %>%
  ungroup()

# Calculate the variance of CPUE by bidn and fltsea
cp_var = cp_all %>%
  group_by(bidn, fltsea, species) %>%
  summarize(var_cpue = var(cpue_day_species, na.rm = TRUE)) %>%
  ungroup()

# Calculate the n of CPUE by bidn and fltsea
cp_n = cp_all %>%
  group_by(bidn, fltsea, species) %>%
  summarize(n_cpue = n()) %>%
  ungroup()

# Combine CPUE data into one file
cpu = cp_n %>%
  left_join(cp_mean, by = c("bidn", "fltsea", "species")) %>%
  left_join(cp_var, by = c("bidn", "fltsea", "species"))

# Calculate the variance of the mean for each species
cpu = cpu %>%
  mutate(var_mean = var_cpue / n_cpue)

# # Check dose creels
# cpu_dose = cpu %>%
#   filter(bidn == 270200L & fltsea == "OO" & species == "man")

# Get rid of CPUEs with n < 3
cpud = cpu %>%
  filter(n_cpue >= 3)

# Get rid of unneeded files
rm(list=(c("cp", "cp_all", "cp_clam", "cp_harv", "cp_mean", "cp_n", "cp_var",
           "cpu", "creel", "cr", "cr_time", "crls", "yrs", "chk_dup_beach",
           "chk_creel", "chk_sub")))

#==============================================================================
# FLIGHT SECTION ----
#==============================================================================

# Pull out flight data without geometry
flt_raw = tibble(bidn = flt_st$bidn,
                 beach_id = flt_st$beach_id,
                 beach_name = flt_st$beach_name,
                 survey_date = flt_st$survey_date,
                 survey_type = flt_st$survey_type,
                 count_time = flt_st$count_time,
                 uclam = flt_st$uclam)

# Get rid of any flight data from beaches not in beach_tide
# We can't expand counts without a low-tide correction
beach_tide = beach_tide %>%
  select(bidn, tide_station, lt_corr)

# Join to flt
flt = flt_raw %>%
  inner_join(beach_tide, by = "bidn")

# Verify number of rows did not change
if (!nrow(flt_raw) == nrow(flt)) {
  cat("\nWARNING: Number of rows changed. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Convert any flight counts made during the Fort Flagler seaweed season
# to zero. The seaweed season runs from April 16th through May 15th.
# LTC counts will not be affected
# Set up begin and end dates for comparison
bdt = as.Date(paste0(current_year, "-04-15"))
edt = as.Date(paste0(current_year, "-05-16"))

# Convert flight counts made during the Fort Flagler seaweed season to zero
flt = flt %>%
  arrange(bidn, as.Date(survey_date)) %>%
  mutate(uclam = if_else(bidn == 250260 &
                           as.Date(survey_date) > bdt & as.Date(survey_date) < edt &
                           survey_type == "aerial", 0L, uclam))

# Verify all were aerial counts and all are now zero
chk_flagler = flt %>%
  arrange(bidn, as.Date(survey_date)) %>%
  filter(bidn == 250260 & as.Date(survey_date) > bdt & as.Date(survey_date) < edt)

#==================================================================================
# Inspect some individual beach counts ----
#==================================================================================

# # Pull out Dosewallips Counts
# dose = flt %>%
#   filter(bidn %in% c(270200))
# mean(dose$uclam, na.rm = TRUE)  # 68.05 in 2017, 59.31 in 2018
#
# # Pull out Potlatch Counts
# potlatch = flt %>%
#   filter(bidn %in% c(270440))
# mean(potlatch$uclam, na.rm = TRUE)  # 32.38 in 2017, 11.68 in 2018

#==================================================================================
# Delete LTC when both LTC and Flight counts exist ----
#==================================================================================

# Sum daily ground and aerial counts to see where both exist
fltsr = flt %>%
  group_by(bidn, survey_date, survey_type) %>%
  summarize(sum_uclam = sum(uclam, na.rm = TRUE)) %>%
  ungroup() %>%
  arrange(bidn, survey_date, survey_type) %>%
  group_by(bidn, survey_date) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(survey_type == "ground" & n_seq == 2L) %>%
  select(bidn, survey_date, survey_type, sum_uclam, n_seq)

# Combine with flt to identify ground counts to delete
chk_delete = fltsr %>%
  left_join(flt, by = c("bidn", "survey_date", "survey_type"))

# Count rows to allow row count comparison
n_flt = nrow(flt)
n_chk = nrow(chk_delete)
n_new_flt = n_flt - n_chk

# Issue warning to inspect comparison
cat("\nWARNING: Please inspect chk_delete dataset to verify deletions!\n\n")

#======================================  Will need section here to select counts to delete !!! =========

# Combine with flt and delete ground counts when both a ground count and aerial count exists
flt = flt %>%
  left_join(fltsr, by = c("bidn", "survey_date", "survey_type")) %>%
  mutate(ground_drop = if_else(n_seq == 2L & survey_type == "ground",
                               "yes", "no")) %>%
  mutate(ground_drop = if_else(is.na(n_seq) & survey_type == "ground", "no", ground_drop))

# # Check ground counts to delete
# chk_delete = flt %>%
#   filter(ground_drop == "yes")

# Get rid of ground counts
flt = flt %>%
  filter(ground_drop == "no") %>%
  select(- c(sum_uclam, n_seq, ground_drop))

# Warning if number of rows incorrect
if (!nrow(flt) == n_new_flt) {
  cat("\nWARNING: Number of rows in flt are unexpected. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows in flt are as expected. Ok to proceed.\n\n")
}

#=============================================================================
# Calculate N observations for the entire season ----
# Will be used to set lower bounds on estimate range in the report
#============================================================================

# Calculate number of observations and harvesters over the entire year
flt_obs = flt %>%
  group_by(bidn, beach_name, survey_date) %>%
  summarize(s_uclam = sum(uclam, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(bidn) %>%
  summarize(n_obs = n(), sum_harv = sum(s_uclam, na.rm = TRUE)) %>%
  ungroup()

#=============================================================================
# EXPAND FLIGHT COUNTS AND LTCS USING EGRESS RATIOS ----
# Step 1: Convert obs times to minutes from low to match egress TimeInt
# Step 2: Subset obs times, one file for each model
# Step 3: Merge obs times with egress ratios
# Step 4: Expand obs counts by egress ratios
#============================================================================

# Step 1: Convert obs times to minutes from midnight
flts = flt %>%
  mutate(obs_min = substr(count_time, 12, 16)) %>%
  mutate(obs_min = as.integer(gsub(":", "", obs_min))) %>%
  mutate(obs_time = obs_min) %>%
  mutate(obs_min = mil_to_minutes(obs_min))

# Check tide_strata counts
table(tide$tide_strata, useNA = "ifany")

# Pull out matching survey_date in tide and filter to only needed strata
tide = tide %>%
  filter(tide_strata %in% c("ELOW", "HIGH", "LOW", "PLUS")) %>%
  mutate(survey_date = substr(tide_date, 1, 10)) %>%
  arrange(tide_date, tide_station)

# Create tide_check dataset
tide_check = tide

# Check for duplicate strata per day
chk_dup_strata = tide_check %>%
  group_by(survey_date, tide_station) %>%
  filter(duplicated(tide_strata)) %>%
  ungroup() %>%
  select(survey_date)

# Get all dates in chk_dup_strata
tide_check = chk_dup_strata %>%
  left_join(tide_check, by = c("survey_date")) %>%
  distinct()

# # Original...Incorrect
# # Identify times of observations on dates with duplicate strata
# dup_dates = tide_check %>%
#   select(survey_date, tide_station, tide_tm = tide_time, tide_ht = tide_height, td_strata = tide_strata) %>%
#   left_join(flts, by = c("survey_date", "tide_station")) %>%
#   mutate(count_time = as.integer(count_time)) %>%
#   mutate(count_time = if_else(is.na(count_time), 10000L, count_time)) %>%
#   mutate(time_diff = abs(tide_tm - count_time)) %>%
#   group_by(survey_date, tide_station) %>%
#   mutate(min_diff = min(time_diff)) %>%
#   filter(time_diff == min_diff) %>%
#   mutate(drop_row = "yes") %>%
#   select(survey_date, tide_station, tide_time = tide_tm, tide_height = tide_ht,
#          tide_strata = td_strata, drop_row)

# Identify times of observations on dates with duplicate strata. filter to rows we want to dump
dup_dates = tide_check %>%
  select(survey_date, tide_station, tide_tm = tide_time, tide_ht = tide_height, td_strata = tide_strata) %>%
  left_join(flts, by = c("survey_date", "tide_station")) %>%
  mutate(count_time = as.integer(obs_min)) %>%
  mutate(count_time = if_else(is.na(count_time), 10000L, count_time)) %>%
  mutate(time_diff = abs(tide_tm - count_time)) %>%
  group_by(survey_date, tide_station) %>%
  mutate(min_diff = min(time_diff)) %>%
  filter(!time_diff == min_diff) %>%
  mutate(drop_row = "yes") %>%
  select(survey_date, tide_station, tide_time = tide_tm, tide_height = tide_ht,
         tide_strata = td_strata, drop_row)

# Combine with flts and drop indicated row
tides = tide %>%
  left_join(dup_dates, by = c("survey_date", "tide_station", "tide_time",
                              "tide_height", "tide_strata")) %>%
  mutate(drop_row = if_else(is.na(drop_row), "no", drop_row)) %>%
  filter(drop_row == "no") %>%
  select(- drop_row)

# Add tide data so strata can be defined
tdflts = flts %>%
  left_join(tides, by = c("survey_date", "tide_station")) %>%
  arrange(bidn, tide_date, obs_min) %>%
  select(bidn, beach_id, beach_name, survey_date, survey_type,
         obs_time, uclam, tide_station, lt_corr, obs_min, tide_time,
         tide_height, tide_strata)

# Warning if new rows created
if (!nrow(tdflts) == nrow(flts)) {
  cat("\nWARNING: Extra rows created. Do not pass go!\n\n")
} else{
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Step 2b: Compute time interval for beach counts
tdflts = tdflts %>%
  mutate(obs_min = as.integer(obs_min)) %>%
  mutate(low_min = tide_time + lt_corr) %>%
  mutate(egress_interval = if_else(obs_min == 0L, 9999L, obs_min - low_min))

# # Get list of egress_model_names
# unique(egress$egress_model_name)

# Add egress ratios and egress variance
fltex = tdflts %>%
  mutate(egress_model_name = case_when(
    !bidn %in% c(250260, 250470, 270300, 270440, 270442,
                 270310, 270460, 270200, 270201, 270286, 270480,
                 270480) & !tide_strata == "ELOW" ~ "Normal non-ELOW",
    !bidn %in% c(250260, 250470, 270300, 270440, 270442,
                 270310, 270460, 270200, 270201, 270286, 270480,
                 270480) & tide_strata == "ELOW" ~ "Normal ELOW",
    bidn %in% c(250260, 250470, 270300, 270440, 270442,
                270310) ~ "Early Peak",
    bidn %in% c(270200, 270201, 270286, 270480) ~ "High Peak",
    bidn == 270460 ~ "Twanoh State Park"))

# Check for missing egress_model_names
if (any(is.na(fltex$egress_model_name))) {
  cat("\nWARNING: Some egress_model names missing. Do not pass go!\n\n")
} else {
  cat("\nAll egress model names present. Ok to proceed.\n\n")
}

# Check for missing egress_model_intervals
if (any(is.na(fltex$egress_interval))) {
  cat("\nWARNING: Some egress_model names missing. Do not pass go!\n\n")
} else {
  cat("\nAll egress model names present. Ok to proceed.\n\n")
}

# Join flt data and egress models
fltex = fltex %>%
  left_join(egress, by = c("egress_model_name", "egress_interval")) %>%
  mutate(clam = if_else(uclam == 0.0, 0.0, uclam / model_ratio)) %>%
  select(bidn, beach_id, beach_name, survey_date, obs_time, uclam, survey_type,
         uclam, tide_strata, egress_interval, model_ratio, model_variance, clam)

# Check for missing values for clam....There was one missing time value
# Edited directly in the DB.. Needed 18:05 for 7/8 DNR-24
# Used: dx = as.POSIXct("2019-07-08 18:05", tz = "America/Los_Angeles")
# then: with_tz(dx, tzone = "UTC") to get correct time for editing.
if (any(is.na(fltex$clam))) {
  cat("\nWARNING: Some expanded clam counts missing. Do not pass go!\n\n")
} else {
  cat("\nAll expanded clam counts present. Ok to proceed.\n\n")
}

# Get rid of unneeded files
rm(list=c("chk_delete", "chk_dup_strata", "chk_flagler", "dup_dates",
          "flt", "flt_raw", "flts", "fltsr", "no_flt_crl_one",
          "no_flt_crl_two", "no_flt_seas", "tide_check", "tide"))

#=================================================================================
# COMPUTE TOTAL DAILY EFFORT AND VARIANCE OF DAILY EFFORT ----
#=================================================================================

# Summarize counts, add model parameters. If model missing...na.rm()
dflt = fltex %>%
  group_by(bidn, survey_date) %>%
  summarize(uclam = sum(uclam),
            ehatdh = sum(clam),
            model_ratio = mean(model_ratio, na.rm = TRUE),
            model_variance = mean(model_variance, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(model_ratio = if_else(is.nan(model_ratio), NA_real_, model_ratio)) %>%
  mutate(model_variance = if_else(is.nan(model_variance), NA_real_, model_variance)) %>%
  select(bidn, survey_date, uclam, ehatdh, model_ratio, model_variance)

# Get unique tide_strata variable
ustrata = fltex %>%
  select(bidn, survey_date, tide_strata) %>%
  distinct()

# Add to dflt
dflt = dflt %>%
  left_join(ustrata, by = c("bidn", "survey_date"))

# Calculate variance on daily all-day effort (delta method)
dflt$vehatdh = dflt$model_variance * ((dflt$uclam ^2) / (dflt$model_ratio ^4))

#====================================================================================
# COMPUTE SEASON-LONG "MEAN" DAILY EFFORT WITHIN EACH TIDE STRATA AND FLT SEASON ----
#====================================================================================

# Pull out seasons data for the current year
flight_seas = both_seas %>%
  filter(year == current_year) %>%
  select(bidn, beach_name, year, begin, end, fltsea = crlsea)

# Check seasons
table(flight_seas$fltsea, useNA = "ifany")

# Combine with dflt
dsflt = dflt %>%
  full_join(flight_seas, by = "bidn") %>%
  mutate(ok = if_else(as.Date(survey_date) >= as.Date(begin) &
                        as.Date(survey_date) <= as.Date(end),
                      "in", "out")) %>%
  filter(ok == "in") %>%
  select(bidn, beach_name, survey_date, uclam, ehatdh, model_ratio,
         model_variance, tide_strata, vehatdh, year, begin, end, fltsea) %>%
  arrange(bidn, survey_date)

# For each stratum and fltsea, compute mean and variance of daily effort,
# sum of effort variance, and n sample days
cflt = dsflt %>%
  group_by(bidn, fltsea, tide_strata) %>%
  summarize(Eh.mn = mean(ehatdh, na.rm = TRUE),
            sh2 = var(ehatdh, na.rm = TRUE),
            dh = n(),
            sumvehatdh = sum(vehatdh, na.rm = TRUE)) %>%
  ungroup()

# Prepare stflt; to be used for effort and variance of effort computation
stflt = cflt %>%
  select(- sh2)

# Combine tides and seasons data to get Dh (number of daylight tides by BIDN, stratum, and fltsea)
flt_seas = flight_seas %>%
  select(bidn, begin, end, fltsea)

# Pull Seattle tides to calculate number of strata in each fltsea category
tdate = tides %>%
  filter(tide_station == "Seattle") %>%
  select(tide_date, s_time = tide_time, s_height = tide_height,
         tide_strata)

# In 2019 there was one case of two clammable tides in one day
# Identify both tides for the day...then select the best one to keep
chk_td = tdate %>%
  mutate(tide_dt = as.Date(tide_date)) %>%
  arrange(tide_dt, desc(s_height)) %>%
  group_by(tide_dt) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1)

chk_tide = tdate %>%
  mutate(tide_dt = as.Date(tide_date)) %>%
  filter(as.Date(tide_date) == chk_td$tide_dt) %>%
  arrange(tide_dt, desc(s_height)) %>%
  group_by(tide_dt) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq == 1)

# Issue warning if duplicated tide exists
if (nrow(chk_tide) > 0 ) {
  cat("\nWARNING. Duplicate tide dates detected. Use code below. Inspect!\n\n")
} else {
  cat("\nNo duplicated tide dates. Ok to proceed.\n\n")
}

# Select the worst tide to delete...there was one duplicate tide in 2019
del_tide = chk_tide %>%
  filter(n_seq == 1L) %>%
  select(tide_date)

# Dump the dup tide
tdate = tdate %>%
  anti_join(del_tide, by = "tide_date")

# Combine flt_seas and tdate
tdsdh = merge(flt_seas, tdate)

# Trim to only include tide dates within season designations
tdsdh = tdsdh %>%
  mutate(tide_date = substr(tide_date, 1, 10)) %>%
  mutate(ok = if_else(as.Date(tide_date) >= as.Date(begin) &
                        as.Date(tide_date) <= as.Date(end),
                      "in", "out")) %>%
  filter(ok == "in") %>%
  select(bidn, begin, end, fltsea, tide_date, s_time,
         s_height, tide_strata)

# Trim to only count tides within Mar-Oct season
tdsdh = tdsdh %>%
  mutate(month = as.integer(substr(tide_date, 6, 7))) %>%
  filter(month > 2 & month < 10)

# Compute the number of available tide days by BIDN
tdsn = tdsdh %>%
  group_by(bidn, tide_strata, fltsea) %>%
  summarize(Dh = n()) %>%
  select(bidn, tide_strata, fltsea, Dh) %>%
  ungroup()

#=============================================================================
# COMPUTE SEASON-LONG "TOTAL" EFFORT WITHIN EACH STRATA AND FLTSEASON ----
#=============================================================================

# Combine tides and effort data
steff = stflt %>%
  left_join(tdsn, by = c("bidn", "tide_strata", "fltsea")) %>%
  mutate(Ehatt = Dh * Eh.mn)

#====================================================================================
# COMPUTE SEASON LONG VARIANCE OF "MEAN" EFFORT WITHIN EACH STRATA AND FLTSEASON ----
#====================================================================================

# Combine effort stats for variance computation
vbd = cflt %>%
  select(bidn, fltsea, tide_strata, sh2)

# Join to steff
veff = steff %>%
  left_join(vbd, by = c("bidn", "fltsea", "tide_strata"))

# Compute components of variance equation for season-long effort by strata and fltsea
# Then compute variance of season-long effort by strata and fltsea from the components
veff = veff %>%
  mutate(fpcorr = (Dh - dh) / Dh) %>%
  mutate(mnBd = sh2 / dh) %>%
  mutate(vEh1 = fpcorr * mnBd) %>%
  mutate(vEh2 = sumvehatdh / (Dh * dh)) %>%
  mutate(vEh.mn = vEh1 + vEh2)

#=======================================================================================
# COMPUTE SEASON LONG VARIANCE OF "TOTAL" EFFORT WITHIN EACH STRATA AND FLTSEASON ----
#=======================================================================================
veff = veff %>%
  mutate(vEht1 = (Dh^2) * (fpcorr * mnBd)) %>%
  mutate(vEht2 = (Dh / dh) * sumvehatdh) %>%
  mutate(vEhatt = vEht1 + vEht2)

#================================================================================
# COMPUTE COMPONENTS OF SATTERTHWAITE'S DEGREES OF FREEDOM APPROXIMATION ----
#================================================================================
veff = veff %>%
  mutate(ah = (Dh * (Dh - dh))/ dh) %>%
  mutate(df1 = ah * sh2) %>%
  mutate(df2 = (df1^2) / (dh - 1))

#===============================================================================
# COMPUTE SEASON-LONG EFFORT AND VARIANCE OF EFFORT BY BIDN AND FLTSEASON ----
# THIS INCLUDES SUMMING DEGREES OF FREEDOM COMPONENTS
#===============================================================================

# Compute season-long stats
sveff = veff %>%
  group_by(bidn, fltsea) %>%
  summarize(Ehatt = sum(Ehatt, na.rm = TRUE),
            Ehatt.var = sum(vEhatt, na.rm = TRUE),
            df1 = sum(df1, na.rm = TRUE),
            df2 = sum(df2, na.rm = TRUE)) %>%
  ungroup() %>%
  select(bidn, fltsea, Ehatt, Ehatt.var, df1, df2)

#=============================================================================
# COMPUTE PLUS TIDES EFFORT BY BIDN AND FLTSEASON ----
#=============================================================================

# Calculate the number of clamtides and plustides by BIDN and fltsea
plus_tides = tdsdh %>%
  mutate(tide = if_else(tide_strata == "PLUS", "ptd", "ctd")) %>%
  group_by(bidn, fltsea, tide) %>%
  summarize(p_tides = n()) %>%
  ungroup() %>%
  select(bidn, fltsea, tide, p_tides) %>%
  filter(tide == "ptd")

# Calculate the mean all-season effort for combined TdStrata by BIDN and fltsea
teff = dsflt %>%
  group_by(bidn, fltsea) %>%
  summarize(Eh.mn = mean(ehatdh, na.rm = TRUE)) %>%
  ungroup() %>%
  select(bidn, fltsea, Eh.mn)

# Combine plustides and Ehattot data....SHOULD "OC" be included in bottom condition ??????
ptot = teff %>%
  left_join(plus_tides, by = c("bidn", "fltsea")) %>%
  select(bidn, fltsea, Eh.mn, p_tides) %>%
  mutate(pEh.mn = case_when(
    fltsea == "OO" ~ 0.16 * Eh.mn,
    fltsea == "CO" | fltsea == "OC" ~ 0.08 * Eh.mn,
    fltsea == "CC" ~ 0.08 * Eh.mn,
    !fltsea %in% c("OO", "CO", "CC") ~ 0.0)) %>%
  mutate(plus_effort = p_tides * pEh.mn)

#==========================================================================
# COMPUTE WINTER EFFORT BY BIDN AND FLTSEASON ----
#==========================================================================

# Create dataframe of winter effort BIDNs. Includes beaches where winter effort
# has been observed. List also includes Duckabush, Quilcene Tidelands, and
# North Bay...where winter effort is suspected
winter_effort = tibble(bidn = c(250050,250055,250057,250300,250400,250410,250470,250510,
                                250512,260240,260510,270170,270171,270201,270200,270230,270286,
                                270297,270300,270310,270312,270380,270410,270440,270442,
                                270460,270480,270500,270900,280711))

## Define winter rates to use for different fltseason combinations
winter_effort = winter_effort %>%
  mutate(OOrate = 0.05) %>%
  mutate(COrate = 0.025) %>%
  mutate(OCrate = 0.025) %>%
  mutate(CCrate = 0.025)

# Combine winter rate beaches with effort data by BIDN and fltseason
seff = sveff %>%
  left_join(winter_effort, by = c("bidn")) %>%
  mutate(OOrate = if_else(is.na(OOrate), 0.0, OOrate)) %>%
  mutate(COrate = if_else(is.na(COrate), 0.0, COrate)) %>%
  mutate(OCrate = if_else(is.na(OCrate), 0.0, OCrate)) %>%
  mutate(CCrate = if_else(is.na(CCrate), 0.0, CCrate))

# Winter effort includes plus tides effort, so add in plus tides effort first
peff = ptot %>%
  select(bidn, fltsea, plus_effort)

# Join with seff, then compute sum of season-long effort and plus tides effort
seff = seff %>%
  left_join(peff, by = c("bidn", "fltsea")) %>%
  mutate(Ehattp = Ehatt + plus_effort)

# Check to see if fltseason designations exist during winter
# wbegin and wend were defined at the top of the program
winter_seas = both_seas %>%
  filter(year == current_year) %>%
  mutate(winter_begin = winter_begin) %>%
  mutate(winter_end = winter_end) %>%
  mutate(winter_season = if_else(as.Date(winter_end) <= as.Date(begin) &
                                   as.Date(winter_begin) >= as.Date(end), 0L, 1L))

# Sum up to identify flight season designations that exist in winter
winter_seas = winter_seas %>%
  mutate(fltsea = crlsea) %>%
  group_by(bidn, fltsea) %>%
  summarize(win_sea = sum(winter_season, na.rm = TRUE)) %>%
  ungroup() %>%
  # Change wsea values to either 1 for exists or 0 for does not exist
  mutate(win_sea = if_else(win_sea == 0, 0, 1))

# Add winter_season data to effort data
seff = seff %>%
  left_join(winter_seas, by = c("bidn", "fltsea")) %>%
  mutate(winter_effort = case_when(
    fltsea == "OO" ~ Ehattp * OOrate * win_sea,
    fltsea == "CO" ~ Ehattp * COrate * win_sea,
    fltsea == "OC" ~ Ehattp * OCrate * win_sea,
    fltsea == "CC" ~ Ehattp * CCrate * win_sea,
    !fltsea %in% c("OO", "CC", "OC", "CC") ~ 0.0))

#==============================================================================
# COMPUTE TOTAL "YEAR-LONG" EFFORT BY BIDN AND FLTSEASON ----
#==============================================================================

# Add winter effort to total effort
seff = seff %>%
  mutate(Ehattpw = Ehattp + winter_effort)

#=========================================================================
# COMPUTE TOTAL "YEAR-LONG" HARVEST ON EACH BEACH BY FLTSEASON ----
#=========================================================================

# Pull out needed variables for next steps
ehatgt = seff %>%
  select(bidn, fltsea, Ehatt.var, df1, df2, Ehatpw = Ehattpw)

# Compute total catch by BIDN and fltseason
# We include plus tides effort and winter effort in this step
ehatgt = ehatgt %>%
  mutate(season = fltsea)

# Merge by fltsea, make sure appropriate cpue is matched with flt data
# We only have cpue in OO and CO fltsea designations
ehatgt = ehatgt %>%
  mutate(fltsea = case_when(
    fltsea == "OO" ~ "OO",
    fltsea == "CO" ~ "CO",
    fltsea == "OC" ~ "OO",
    fltsea == "CC" ~ "OO",
    !fltsea %in% c("OO", "CO", "OC", "CC") ~ "OO"))

# Verify all allowance beach_ids match to flt data -------------------------

# Check for beach_allowance data that are not in the flt file
# RESULT: None needed to be updated
no_flt_allow_one = beach_allow %>%
  filter(!beach_id %in% flt_bch_ids) %>%
  select(beach_id, bidn, beach_name) %>%
  distinct() %>%
  arrange(beach_name)

no_flt_allow_two = beach_allow %>%
  filter(!bidn %in% flt_bidns) %>%
  select(beach_id, bidn, beach_name) %>%
  distinct() %>%
  arrange(beach_name)

# Warning if any beaches need to be reassigned
if (nrow(no_flt_allow_one) > 0 | nrow(no_flt_allow_two) > 0 ) {
  cat("\nWARNING: Some allowance data will not match flight data. Do not pass go!!!\n\n")
} else {
  cat("\nAll allowance data matches beach_ids and bidns in flight data. Ok to proceed.\n\n")
}

# # Manually inspect flt_st to identify correct beach_id and bidn for all cases in
# # no_flt_allow_one and no_flt_allow_two. Then update both beach_id and bidn values as needed.
# # Need to set beach_id to match flight file. Allowance data are non-spatial.
# # Flight data sets the beach_id and bidn value for the year and are spatially joined.
# beach_allow = beach_allow %>%
#   # Dosewallips
#   mutate(bidn = if_else(bidn == 270200L, 270201L, bidn)) %>%
#   mutate(beach_id = if_else(beach_id == "d65072f2-21a5-4d5c-9c17-aa6c0e65ce8c",
#                             "09cdc9d8-4741-46e4-810d-4f80468fbd48", beach_id)) %>%
#   # Quilcene Bay
#   mutate(bidn = if_else(bidn == 270900L, 270500L, bidn)) %>%
#   mutate(beach_id = if_else(beach_id == "f0439944-d0e9-4c9f-9888-bf2db6b4a4e7",
#                             "d6237291-f4fb-40d7-a5d4-0dbe2c6688f5", beach_id))
#
# # The other two beaches...Samish Is Rec Area and Spencer Spit are not
# # in the flight files, so no need to correct

# End beach_id check section -----------------------------------------------------

# Pull out beach_allowance status
beach_status = beach_allow %>%
  select(bidn, beach_name, beach_status = beach_status_code, report_type) %>%
  distinct()

# Verify no duplicate bidns
if (any(duplicated(beach_status$bidn))) {
  cat("\nWARNING: Duplicated bidns detected. Do not pass go!")
} else {
  cat("\nNo duplicated bidns. Ok to proceed.\n\n")
}

# Visually inspect bidns
sort(unique(beach_status$bidn))

#=================================================================================
# SECTION TO DUMP UNNEEDED CREEL DATA....For beaches that have CPUE but are not
# listed in the allowance file
#=================================================================================

# Add beach status and report type info
cpue = cpud %>%
  left_join(beach_status, by = "bidn")

# WARNING for missing status info.
# Use add_missing_beach_allowance_2018.R to add missing beach_allowance data
chk_status = cpue %>%
  filter(is.na(beach_status)) %>%
  select(bidn) %>%
  distinct() %>%
  left_join(bidns, by = "bidn") %>%
  select(bidn, beach_name) %>%
  distinct()

if (nrow(chk_status) > 0 ) {
  cat("\nWARNING: Data missing in beach_allow table. Verify creel data from BIDNs below can be deleted!\n\n")
  chk_status
} else {
  cat("\nAll required data present in beach_allow table. Ok to proceed.\n\n")
}

# Trim cpu to only actively managed beaches...so if you want catch estimates make sure beach is in allowance file
cat("\nAll creel data from beaches not in allowance file will be deleted here!\n\n")
cpue = cpud %>%
  left_join(beach_status, by = "bidn") %>%
  filter(!is.na(beach_status))

# Combine CPUE and Flight data
catch = ehatgt %>%
  left_join(cpue, by = c("bidn", "fltsea"))

#=========================================================================
# GET SUBSTITUTION VALUES FOR BEACHES WITHOUT CREELS ----
#=========================================================================
# WE NEED TO DO THIS AFTER ADDING IN FLIGHT DATA
# OTHERWISE WE WOULD NOT KNOW WHAT CPUE IS MISSING
# This will change each year depending on where we are short CPUE data
# Step 1: Subset appropriate substitution beach data from cpu
# Step 2: Change BIDN and BeachName to correct values

# Find out where we need CPUE data
cpno = catch %>%
  filter(is.na(beach_name)) %>%
  select(- c(beach_name, beach_status)) %>%
  inner_join(beach_status, by = "bidn") %>%
  filter(Ehatpw > 0.0) %>%
  arrange(bidn, season) %>%
  filter(!report_type.y == "External effort") %>%
  select(bidn, beach_name, season, beach_status, report_type = report_type.y)

# # Output for Camille and Doug
# write.csv(cpno, "MissingCPUE_2019.csv", row.names = FALSE)
# write.csv(cpue, "AvailableCPUE_2019.csv", row.names = FALSE)

# Issue warning to manually create substitutions
if (nrow(cpno) > 0 ) {
  cat("\nWARNING: Stop and inspect cpno. Manual coding of substitutions needed!!!\n\n")
} else{
  cat("\nNo substitutions needed. Ok to proceed.\n\n")
}

# Inspect cpno to identify beaches and fltsea where substitutions are needed !!!!!

# Change bidn to 270202 & fltsea to 'OO'
sub_one = cpue %>%
  filter(bidn == 270801 & fltsea == "OO") %>%
  mutate(bidn = 270802) %>%
  mutate(beach_name = "E Dabob") %>%
  mutate(fltsea = "OO")

# # Change BIDN to 281041
# sub_three = cpue %>%
#   filter(bidn == 281043 & fltsea == "OO") %>%
#   mutate(bidn = 281041) %>%
#   mutate(beach_name = "Oakland Bay Ch + Flats")

# # SPECIAL SUBSTITUTION FOR 2009 ONLY!!!!
# # HARD-CODE SUBSTITUTION VALUES FOR SARATOGA PASS (240430)
# # VALUES WERE GENERATED USING SaratogaCPUE.R and can be found
# # in the Proofing/Alex folder
# s4 = read.csv(paste(paths[[3]],'\\SaratogaCPUE_2009.csv',sep=''))
# s4 = subset(s4, BIDN==240430)

# Combine substitution data
csubs = rbind(sub_one)

# Add to cpue
cpue = rbind(cpue, csubs)

# Clean up
rm(list = c("sub_one", "csubs"))

# Combine CPUE and Flight data again now that subs have been added
catch = ehatgt %>%
  left_join(cpue, by = c("bidn", "fltsea"))

# Create version of catch with only actively managed and Statex beaches
# for proofing. Make sure no rows remain with NA values for CPUE
# For 2009 there were three beaches where we did not have sufficient creel
#  data and no substitution values (240430: Saratoga Pass, 280820: DNR-24,
#                                   280830: McMicken Island)
cproof = catch %>%
  filter(!is.na(beach_status)) %>%
  select(bidn, beach_name, beach_status, fltsea, Ehatt.var,
         df1, df2, Ehatpw, season, species, n_cpue, mean_cpue,
         var_cpue, var_mean)

# Generate total catch by BIDN species and fltseason
# This step DOES include plus and winter effort
catch = catch %>%
  mutate(season_catch = Ehatpw * mean_cpue) %>%
  mutate(fltsea = season) %>%
  select(- season)

#==========================================================================================
# FINAL SET OF COMPUTATIONS FOR GRAND TOTALS OF TOTAL "YEAR-LONG" EFFORT AND HARVEST ----
# Includes plus and winter effort
#==========================================================================================

# Calculate season-long grand-totals of effort
chatpw = catch %>%
  select(bidn, Ehatpw) %>%
  distinct() %>%
  group_by(bidn) %>%
  summarize(total_effort = sum(Ehatpw, na.rm = TRUE)) %>%
  ungroup()

#==========================================================================================
# COMPUTE VARIANCE OF HARVEST ON EACH BEACH BY BIDN STRATA AND FLTSEASON ----
# We DO NOT include plus tides effort and winter effort in these steps
#==========================================================================================

# Trim effort data to only needed variables
vflt = veff %>%
  select(bidn, fltsea, tide_strata, Eh.mn, Dh, vEh.mn) %>%
  mutate(Ehat = Eh.mn * Dh) %>%     # Compute effort
  mutate(season = fltsea) %>%
  mutate(fltsea = case_when(
    fltsea == "OO" ~ "OO",
    fltsea == "CO" ~ "CO",
    fltsea == "OC" ~ "OO",
    fltsea == "CC" ~ "OO",
    !fltsea %in% c("OO", "CC", "OC", "CC") ~ "OO"))

# Step 1: Merge by fltsea. Make sure appropriate cpue is matched with flt data
# We only have cpue in OO and CO fltsea designations
# Create season variable to preserve original fltsea designations

# Add cpue data
vck = vflt %>%
  left_join(cpue, by = c("bidn", "fltsea")) %>%
  mutate(fltsea = season) %>%
  select(- season)

# Calculate mean daily harvest and variance of mean daily harvest
vck = vck %>%
  mutate(mean_catch = Eh.mn * mean_cpue) %>%
  mutate(v1 = (mean_cpue^2) * vEh.mn) %>%
  mutate(v2 = (Eh.mn^2) * var_mean) %>%
  mutate(v3 = vEh.mn * var_mean) %>%
  mutate(var_catch = v1 + v2 - v3)

# Calculate total season-long harvest and variance of harvest
vck = vck %>%
  mutate(season_catch = mean_catch * Dh) %>%
  mutate(var_season_catch = var_catch * Dh^2)

# Calculate the sum and variance of season-long HARVEST by BIDN
svck = vck %>%
  group_by(bidn, species) %>%
  summarize(Ehat = sum(Ehat, na.rm = TRUE),
            sum_catch = sum(season_catch, na.rm = TRUE),
            var_catch = sum(var_season_catch, na.rm = TRUE)) %>%
  ungroup()

# Calculate the sum and variance of season-long EFFORT by BIDN
sveff2 = veff %>%
  group_by(bidn) %>%
  summarize(Ehatt = sum(Ehatt, na.rm = TRUE),
            vEhatt = sum(vEhatt, na.rm = TRUE),
            df1 = sum(df1, na.rm = TRUE),
            df2 = sum(df2, na.rm = TRUE)) %>%
  ungroup()

# Combine effort variance and df data with harvest CI data
chatt = sveff2 %>%
  left_join(svck, by = "bidn") %>%
  # Standard error
  mutate(Ehatt.se = sqrt(vEhatt)) %>%
  # Degrees of freedom
  mutate(df = df1^2 / df2) %>%
  # t-value
  mutate(tval = qt(0.025, df, lower.tail = FALSE)) %>%
  # 95% CI on effort
  mutate(ci.eff = tval * Ehatt.se) %>%
  # Percent error on effort estimates
  # We want to compute without plus or winter effort
  mutate(eff.err = (ci.eff / Ehatt) * 100) %>%
  # 95% CIs on season-long harvest
  mutate(catch.ci = sqrt(var_catch) * tval) %>%
  # Compute percent error on catch estimates
  # We want to compute without plus or winter effort
  mutate(catch.err = (catch.ci / sum_catch) * 100)

# Create file for proofing output that includes t-value
chattex = chatt

#================================================================================
# Compute CIs using percent error ----
# Then apply the intervals to the catch that includes plus and winter effort
#================================================================================

# Combine percent error data with grand total of catch using plus and winter effort
chatt = chatt %>%
  select(bidn, Ehatt, vEhatt, species, sum_catch, eff.err, catch.err)

# Join effort data that includes plus and winter effort with catch estimates
chatt_all = chatt %>%
  left_join(chatpw, by = "bidn") %>%
  mutate(eff.ci = total_effort * (eff.err / 100)) %>%
  mutate(species_catch.err = total_effort * (catch.err / 100)) %>%
  mutate(catch.err = if_else(is.nan(catch.err), NA_real_, catch.err)) %>%
  select(bidn, total_effort, effort_error = eff.err, species,
         catch = sum_catch, catch_error = catch.err)

#================================================================================
# Combine values to generate Wolfe-Shine summed ----
#================================================================================

# Pull out Wolfe and Shine
chatt_wolfe = chatt_all %>%
  filter(bidn %in% c(250510, 250512)) %>%
  arrange(species)

# Compute combined effort values
wolfe_effort = chatt_wolfe %>%
  select(bidn, total_effort, effort_error) %>%
  distinct() %>%
  mutate(bidn = 250511L) %>%
  group_by(bidn) %>%
  summarize(total_effort = sum(total_effort, na.rm = TRUE),
            effort_error = mean(effort_error, na.rm = TRUE)) %>%
  ungroup()

# Compute combined catch values
wolfe_catch = chatt_wolfe %>%
  select(bidn, species, catch, catch_error) %>%
  mutate(catch = if_else(bidn == 250512L & species == "oys", NA_real_, catch)) %>%
  mutate(catch_error = if_else(bidn == 250512L & species == "oys", NA_real_, catch_error)) %>%
  mutate(bidn = 250511L) %>%
  group_by(bidn, species) %>%
  summarize(catch = sum(catch, na.rm = TRUE),
            catch_error = mean(catch_error, na.rm = TRUE)) %>%
  ungroup()

# Combine
wolfe_catch = wolfe_effort %>%
  full_join(wolfe_catch, by = "bidn")

# Add to chatt_all
chatt_all = rbind(chatt_all, wolfe_catch)
chatt_all = chatt_all %>%
  arrange(bidn, species)

# Add Wolfe-Shine combo to flt_obs
wolfe_obs = flt_obs %>%
  filter(bidn %in% c(250510, 250512)) %>%
  mutate(bidn = 250511L) %>%
  group_by(bidn) %>%
  summarize(n_obs = max(n_obs, na.rm = TRUE),
            sum_harv = sum(sum_harv, na.rm = TRUE)) %>%
  ungroup()

# Combine with flt_obs
flt_obs = rbind(flt_obs, wolfe_obs)
flt_obs = flt_obs %>%
  arrange(bidn)

#================================================================================
# Compute effort and catch CI ranges and add N ----
#================================================================================

# Join catch and effort data to flt_obs
chatt_range = chatt_all %>%
  left_join(flt_obs, by = "bidn") %>%
  # Effort range
  mutate(effort_ci = total_effort * (effort_error / 100)) %>%
  mutate(up_effort_ci = as.integer(total_effort + effort_ci)) %>%
  mutate(lo_effort_ci = total_effort - effort_ci) %>%
  mutate(lo_effort_ci = if_else(sum_harv > lo_effort_ci, as.integer(sum_harv), as.integer(lo_effort_ci))) %>%
  mutate(up_effort_ci = trimws(as.character(comma(up_effort_ci, format = "d")))) %>%
  mutate(lo_effort_ci = trimws(as.character(comma(lo_effort_ci, format = "d")))) %>%
  mutate(effort_range = paste0(up_effort_ci, "-", lo_effort_ci)) %>%
  mutate(effort_range = stri_replace_all_fixed(effort_range, " ", "")) %>%
  mutate(effort_range = stri_replace_all_fixed(effort_range, "NA-NA", NA_character_)) %>%
  mutate(effort_range = stri_replace_all_fixed(effort_range, "-", " - ")) %>%
  select(- c(effort_ci, up_effort_ci, lo_effort_ci)) %>%
  # Catch range
  mutate(catch_ci = catch * (catch_error / 100)) %>%
  mutate(up_catch_ci = as.integer(catch + catch_ci)) %>%
  mutate(lo_catch_ci = as.integer(catch - catch_ci)) %>%
  mutate(lo_catch_ci = if_else(lo_catch_ci < 0L , 0L, lo_catch_ci)) %>%
  mutate(up_catch_ci = trimws(as.character(comma(up_catch_ci, format = "d")))) %>%
  mutate(lo_catch_ci = trimws(as.character(comma(lo_catch_ci, format = "d")))) %>%
  mutate(catch_range = paste0(up_catch_ci, "-", lo_catch_ci)) %>%
  mutate(catch_range = stri_replace_all_fixed(catch_range, " ", "")) %>%
  mutate(catch_range = stri_replace_all_fixed(catch_range, "NA-NA", NA_character_)) %>%
  mutate(catch_range = stri_replace_all_fixed(catch_range, "-", " - ")) %>%
  select(- c(catch_ci, up_catch_ci, lo_catch_ci)) %>%
  # Format output values
  mutate(display_effort = as.character(comma(total_effort, format = "d"))) %>%
  mutate(display_effort_error = as.character(comma(effort_error, digits = 1, format = "f"))) %>%
  mutate(display_effort_error = if_else(display_effort_error == "NaN", NA_character_, display_effort_error)) %>%
  mutate(display_catch = as.character(comma(catch, format = "d"))) %>%
  mutate(display_catch_error = as.character(comma(catch_error, digits = 1, format = "f"))) %>%
  mutate(display_catch_error = if_else(display_catch_error == "NA", NA_character_, display_catch_error)) %>%
  mutate(effort_error = if_else(is.nan(effort_error), NA_real_, effort_error))

#==========================================================================
# OUTPUT SECTION ----
#==========================================================================

# Add bidnfo allocation data
bidnfo = beach_allow %>%
  select(bidn, beach_status = beach_status_code,
         estimate_type, species_group = species_group_code,
         report_type, allowable_harvest)

# Get count of bidns to validate spread function below
length(unique(bidnfo$bidn))
length(unique(chatt_all$bidn))

# Pivot out allowance numbers, one line per bidn
allow = bidnfo %>%
  spread(species_group, allowable_harvest) %>%
  select(-na)

# Pull out harvest numbers, one line per bidn
harv_effort = chatt_range %>%
  select(bidn, total_effort, effort_error, display_effort, display_effort_error, effort_range, n_obs) %>%
  distinct()

# Pivot out catch numbers, one line per bidn
harv_catch = chatt_range %>%
  select(bidn, species, catch) %>%
  spread(species, catch)

# Pivot out catch error numbers, one line per bidn
harv_catch_error = chatt_range %>%
  select(bidn, species, catch_error) %>%
  spread(species, catch_error)

# Pivot out display catch numbers, one line per bidn
harv_display_catch = chatt_range %>%
  select(bidn, species, display_catch) %>%
  spread(species, display_catch)

# Pivot out display catch error numbers, one line per bidn
harv_display_catch_error = chatt_range %>%
  select(bidn, species, display_catch_error) %>%
  spread(species, display_catch_error)

# Pivot out catch range numbers, one line per bidn
harv_catch_range = chatt_range %>%
  select(bidn, species, catch_range) %>%
  spread(species, catch_range)

harvest = harv_effort %>%
  left_join(harv_catch, by = "bidn") %>%
  select(bidn, total_effort, effort_error, display_effort, display_effort_error,
         effort_range, n_obs, butter_catch = but, cockle_catch = coc,
         eastern_catch = ess, geoduck_catch = geo, horse_catch = hor,
         manila_catch = man, native_catch = nat, oyster_catch = oys) %>%
  left_join(harv_catch_error, by = "bidn") %>%
  select(bidn, total_effort, effort_error, display_effort, display_effort_error,
         effort_range, n_obs, manila_catch, manila_error = man,
         native_catch, native_error = nat, butter_catch, butter_error = but,
         horse_catch, horse_error = hor, cockle_catch, cockle_error = coc,
         eastern_catch, eastern_error = ess, geoduck_catch, geoduck_error = geo,
         oyster_catch, oyster_error = oys) %>%
  left_join(harv_display_catch, by = "bidn") %>%
  select(bidn, total_effort, effort_error, display_effort, display_effort_error,
         effort_range, n_obs, manila_catch, display_manila_catch = man, manila_error,
         native_catch, display_native_catch = nat, native_error, butter_catch,
         display_butter_catch = but, butter_error, horse_catch, display_horse_catch = hor,
         horse_error, cockle_catch, display_cockle_catch = coc, cockle_error,
         eastern_catch, display_eastern_catch = ess, eastern_error, geoduck_catch,
         display_geoduck_catch = geo, geoduck_error, oyster_catch, display_oyster_catch = oys,
         oyster_error) %>%
  left_join(harv_display_catch_error, by = "bidn") %>%
  select(bidn, total_effort, effort_error, display_effort, display_effort_error,
         effort_range, n_obs, manila_catch, display_manila_catch, manila_error,
         display_manila_error = man, native_catch, display_native_catch, native_error,
         display_native_error = nat, butter_catch, display_butter_catch, butter_error,
         display_butter_error = but, horse_catch, display_horse_catch,
         horse_error, display_horse_error = hor, cockle_catch, display_cockle_catch,
         cockle_error, display_cockle_error = coc, eastern_catch, display_eastern_catch,
         eastern_error, display_eastern_error = ess, geoduck_catch, display_geoduck_catch,
         geoduck_error, display_geoduck_error = geo, oyster_catch, display_oyster_catch,
         oyster_error, display_oyster_error = oys) %>%
  left_join(harv_catch_range, by = "bidn") %>%
  select(bidn, total_effort, effort_error, display_effort, display_effort_error,
         effort_range, n_obs, manila_catch, display_manila_catch, manila_error,
         display_manila_error, manila_range = man, native_catch, display_native_catch,
         native_error, display_native_error, native_range = nat, butter_catch,
         display_butter_catch, butter_error, display_butter_error, butter_range = but,
         horse_catch, display_horse_catch, horse_error, display_horse_error,
         horse_range = hor, cockle_catch, display_cockle_catch, cockle_error,
         display_cockle_error, cockle_range = coc, eastern_catch, display_eastern_catch,
         eastern_error, display_eastern_error, eastern_range = ess, geoduck_catch,
         display_geoduck_catch, geoduck_error, display_geoduck_error, geoduck_range = geo,
         oyster_catch, display_oyster_catch, oyster_error, display_oyster_error,
         oyster_range = oys)

# Convert Wolfe-Shine beach back to 250510
harvest = harvest %>%
  mutate(bidn = if_else(bidn == 250511, 250510, bidn))

# Join to bidnfo
harvest_rep = harvest %>%
  left_join(allow, by = "bidn")

#============================================================================
# Get bidn data with sfma and mng_region ----
#============================================================================

# # Check crs of input polygons
# st_crs(bch_st)
# st_crs(mng_reg_st)
# st_crs(sfma_st)

# Add beach names
bch_info_st = bch_st

# Create beach polygon centroids
bch_point_st = bch_info_st %>%
  mutate(beach_center = st_centroid(geometry)) %>%
  select(bidn, beach_name, beach_center)

# Convert to lat-lon
bch_point_st = st_transform(bch_point_st, 4326)

# Pull out lat-lons
bch_point_st = bch_point_st %>%
  mutate(lon = as.numeric(st_coordinates(beach_center)[,1])) %>%
  mutate(lat = as.numeric(st_coordinates(beach_center)[,2]))

# Get rid of geometry
bch_center = tibble(bidn = bch_point_st$bidn,
                    beach_name = bch_point_st$beach_name,
                    lon = bch_point_st$lon,
                    lat = bch_point_st$lat)

# Check for duplicate bidns
if (any(duplicated(bch_center$bidn))) {
  cat("\nWARNING: Some duplicated bidns. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated bidns. Ok to proceed.\n\n")
}

# Pull out duplicated bidns
chk_bidn_dups = bch_center %>%
  filter(duplicated(bidn)) %>%
  select(bidn) %>%
  left_join(bch_center, by = "bidn")

# Get rid of second occurrance of Toandos
bch_center = bch_center %>%
  group_by(bidn) %>%
  mutate(n_seq = row_number(bidn)) %>%
  ungroup() %>%
  filter(n_seq == 1L) %>%
  select(- n_seq)

# Recreate geometry using lat-lons
bch_center_st = st_as_sf(bch_center, coords = c("lon", "lat"), crs = 4326)

# Convert back to state plane
bch_center_st = st_transform(bch_center_st, 2927)

# Join with mng_reg and sfma polygons
bch_mng_st = st_join(bch_center_st, mng_reg_st)
bch_sfma_st = st_join(bch_mng_st, sfma_st)

# Pull out needed varibles
bch_info = tibble(bidn = bch_sfma_st$bidn,
                  beach_name = bch_sfma_st$beach_name,
                  mng_region = bch_sfma_st$management_region_code,
                  sfma = bch_sfma_st$shellfish_area_code)

# Check for duplicate bidns
if (any(duplicated(bch_info$bidn))) {
  cat("\nWARNING: Some duplicated bidns. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated bidns. Ok to proceed.\n\n")
}

#============================================================================
# Add beach info to harvest. And calculate remaining fields ----
#============================================================================

# Add names, sfma, and mng_reg. Then compute last set of values.
harvest_report = harvest_rep %>%
  left_join(bch_info, by = "bidn") %>%
  mutate(sum_littleneck = as.integer(manila_catch + native_catch)) %>%
  mutate(remaining_littleneck = as.integer(Littleneck - sum_littleneck)) %>%
  mutate(remaining_manila = as.integer(Manila - manila_catch)) %>%
  mutate(remaining_butter = as.integer(Butter - butter_catch)) %>%
  mutate(remaining_cockle = as.integer(Cockle - cockle_catch)) %>%
  mutate(remaining_horse = as.integer(Horse - horse_catch)) %>%
  mutate(remaining_oyster = as.integer(Oyster - oyster_catch)) %>%
  # Format with commas
  mutate(sum_littleneck = comma(sum_littleneck, format = "d")) %>%
  mutate(remaining_littleneck = comma(remaining_littleneck, format = "d")) %>%
  mutate(remaining_manila = comma(remaining_manila, format = "d")) %>%
  mutate(remaining_butter = comma(remaining_butter, format = "d")) %>%
  mutate(remaining_cockle = comma(remaining_cockle, format = "d")) %>%
  mutate(remaining_horse = comma(remaining_horse, format = "d")) %>%
  mutate(remaining_oyster = comma(remaining_oyster, format = "d")) %>%
  mutate(Manila = comma(Manila, format = "d")) %>%
  mutate(Littleneck = comma(Littleneck, format = "d")) %>%
  mutate(Butter = comma(Butter, format = "d")) %>%
  mutate(Cockle = comma(Cockle, format = "d")) %>%
  mutate(Horse = comma(Horse, format = "d")) %>%
  mutate(Oyster = comma(Oyster, format = "d")) %>%
  # Get rid of beaches where no harvest recorded
  #filter(total_effort > 0.0) %>%
  mutate(beach_status = if_else(is.na(beach_status), "Passive", beach_status)) %>%
  select(BIDN = bidn, BeachName = beach_name, Status = beach_status,
         EstimateType = estimate_type, ReportType = report_type,
         MngReg = mng_region, SFMA = sfma, SportEffortEstimate = display_effort,
         EffortPctError = display_effort_error, EffortRange = effort_range,
         EffortSurveys = n_obs, ManilaClamEstimate = display_manila_catch,
         ManilaPctError = display_manila_error, ManilaRange = manila_range,
         SportManilaAllowable = Manila, RemainingManilaAllowable = remaining_manila,
         NativeLittleneckEstimate = display_native_catch, NativePctError = display_native_error,
         NativeRange = native_range, TotalLittleneckEstimate = sum_littleneck,
         SportLittleneckAllowable = Littleneck, RemainingLittleneckAllowable = remaining_littleneck,
         ButterClamEstimate = display_butter_catch, ButterPctError = display_butter_error,
         ButterRange = butter_range, SportButterAllowable = Butter,
         RemainingButterAllowable = remaining_butter, HorseClamEstimate = display_horse_catch,
         HorsePctError = display_horse_error, HorseRange = horse_range, SportHorseAllowable = Horse,
         RemainingHorseAllowable = remaining_horse, CockleEstimate = display_cockle_catch,
         CocklePctError = display_cockle_error, CockleRange = cockle_range,
         SportCockleAllowable = Cockle, RemainingCockleAllowable = remaining_cockle,
         EasternSoftshellEstimate = display_eastern_catch, EasternPctError = display_eastern_error,
         EasternRange = eastern_range, GeoduckEstimate = display_geoduck_catch,
         GeoduckPctError = display_geoduck_error, GeoduckRange = geoduck_range,
         OysterEstimate = display_oyster_catch, OysterPctError = display_oyster_error,
         OysterRange = oyster_range, SportOysterAllowable = Oyster,
         RemainingOysterAllowable = remaining_oyster)

# Identify Wolfe-Shine using arrange and row-number. Then assign new name
harvest_report = harvest_report %>%
  mutate(effort = as.integer(stri_replace_all_fixed(SportEffortEstimate, ",", ""))) %>%
  arrange(BIDN, effort) %>%
  group_by(BIDN) %>%
  mutate(n_seq = row_number(BIDN)) %>%
  ungroup() %>%
  mutate(BeachName = if_else(BIDN == 250510 & n_seq == 2, "Wolfe-Shine SP", BeachName)) %>%
  select(- c(effort, n_seq))


# Calculate time for running program: 47.45288 secs
Sys.time() - strt

#============================================================================
# Output harvest report to excel ----
#============================================================================

# Get rid of NAs in excel output
harvest_report[] = lapply(harvest_report, set_empty)

# Output with styling
num_cols = ncol(harvest_report)
out_name = glue("Summary/HarvestAssessment/data/{current_year}_", "HarvestReport.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "HarvestReport", gridLines = TRUE)
writeData(wb, sheet = 1, harvest_report, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)


# Ran to here...looks good !!!!!!!!!!!!!!!!!!


#============================================================================
# Prepare data for the projection program ----
#============================================================================

# Get plus tides mean effort
pmn = ptot %>%
  mutate(estimation_year = as.integer(current_year)) %>%
  mutate(tide_strata = "PLUS") %>%
  select(beach_number = bidn, estimation_year, tide_strata,
         flight_season = fltsea, mean_effort = pEh.mn,
         tide_count = p_tides)

# Get remaining tides mean effort data
smn = steff %>%
  mutate(estimation_year = as.integer(current_year)) %>%
  select(beach_number = bidn, estimation_year, tide_strata,
         flight_season = fltsea, mean_effort = Eh.mn,
         tide_count = Dh)

# Combine
mean_effort_est = rbind(smn,pmn)

# Add beach info and arrange
mean_effort_est = mean_effort_est %>%
  left_join(bidns, by = c("beach_number" = "bidn"))

# Chock far missing beach_id
any(is.na(mean_effort_est$beach_id))

# Add id and final fields
mean_effort_est = mean_effort_est %>%
  mutate(mean_effort_estimate_id = remisc::get_uuid(nrow(mean_effort_est))) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  arrange(beach_name, flight_season, tide_strata) %>%
  select(mean_effort_estimate_id, beach_id, beach_number, beach_name,
         estimation_year, tide_strata, flight_season, mean_effort,
         tide_count, created_datetime, created_by, modified_datetime,
         modified_by)

# Use the longest dataset for cpue vs cpud
# We want all cpue data...even non-active beaches possibly deleted from cpue above
if (nrow(cpue) > nrow(cpud) ) {
  cpue_dat = cpue
} else {
  cpue_dat = cpud %>%
    mutate(beach_name = NA_character_)
}

# Get cpue data
mean_cpue_est = cpue_dat %>%
  mutate(bidn = as.integer(bidn)) %>%
  select(-beach_name) %>%
  left_join(bidns, by = "bidn") %>%
  arrange(beach_name, fltsea, species)

# Verify no missing beach_id
any(is.na(mean_cpue_est$beach_id))

# Add id and final fields
mean_cpue_est = mean_cpue_est %>%
  mutate(mean_cpue_estimate_id = remisc::get_uuid(nrow(mean_cpue_est))) %>%
  mutate(estimation_year = as.integer(current_year)) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(mean_cpue_estimate_id, beach_id, beach_number = bidn, beach_name,
         estimation_year, flight_season = fltsea, species_code = species,
         survey_count = n_cpue, mean_cpue, created_datetime, created_by,
         modified_datetime, modified_by)

# # Dump previously uploaded values
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbExecute(db_con, glue("delete from mean_effort_estimate where estimation_year = {current_year}"))
# DBI::dbExecute(db_con, glue("delete from mean_cpue_estimate where estimation_year = {current_year}"))
# DBI::dbDisconnect(db_con)
#
# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "mean_effort_estimate", mean_effort_est, row.names = FALSE, append = TRUE)
# DBI::dbWriteTable(db_con, "mean_cpue_estimate", mean_cpue_est, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)
#
# # Dump previously uploaded values
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbExecute(db_con, glue("delete from mean_effort_estimate where estimation_year = {current_year}"))
# DBI::dbExecute(db_con, glue("delete from mean_cpue_estimate where estimation_year = {current_year}"))
# DBI::dbDisconnect(db_con)
#
# Write to shellfish
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "mean_effort_estimate", mean_effort_est, row.names = FALSE, append = TRUE)
# DBI::dbWriteTable(db_con, "mean_cpue_estimate", mean_cpue_est, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)





