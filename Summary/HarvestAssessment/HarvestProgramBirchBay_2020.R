#================================================================================================
# Harvest Program for Birch Bay 2019
#
#  NOTES: 
#    1. Don't forget to update birch_psp_season.xlsx and load new values to DB.
#        
#  ToDo:
# 	1. Make sure and manually edit tide data for 3/15/2008. Tide was at sunset break point so
#      incorrect morning tide is being used for expansions. DONE !!!!
#   2. DOUBLE CHECK ALL SEASONS...
# 
# AS, 2019-12-03
#================================================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
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

# Set the offset year to bring us back to 2008 as start point for data
offset_years = current_year - 2008
offset_years

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
qry = glue("select distinct bb.beach_id, bb.beach_number as bidn, bb.beach_name, ",
           "bb.active_datetime, bb.inactive_datetime, bb.geom as geometry ",
           "from beach_boundary_history as bb ",
           "where date_part('year', bb.active_datetime) >= {current_year - offset_years} ",
           "and date_part('year', bb.inactive_datetime) >= {current_year - offset_years} ",
           "and bb.beach_name = 'Birch Bay SP'")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Explicitly convert timezones
beach_st = beach_st %>% 
  mutate(active_datetime = with_tz(active_datetime, tzone = "America/Los_Angeles")) %>% 
  mutate(inactive_datetime = with_tz(inactive_datetime, tzone = "America/Los_Angeles"))

# Inspect years in the beach_polygons data
unique(as.integer(substr(beach_st$active_datetime, 1, 4)))

# Should be 2008 to current year number of rows
n_rows = current_year - 2007

# Report if insufficient number of rows
if (nrow(beach_st) < n_rows) {
  cat("\nWARNING: Insufficient number of years of beach data. Add needed data.\n\n")
} else {
  cat("\nNo missing beach data. Ok to proceed.\n\n")
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
           "and date_part('year', bb.inactive_datetime) = {current_year} ",
           "and b.local_beach_name = 'Birch Bay SP'")

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
           "and b.local_beach_name = 'Birch Bay SP' ",
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

#===============================================================
# Seasons data ---- Using reg season for creels, psp for effort
#===============================================================

# 4. Get beach season data. Get same number or rows, with or without "and end_date" part
qry = glue("select distinct bs.beach_id, bb.beach_number as bidn, bb.beach_name, ",
           "ss.season_status_code, ss.season_status_description, sg.species_group_code, ",
           "bs.season_start_datetime as season_start, bs.season_end_datetime as season_end, ",
           "bs.season_description, bs.comment_text ",
           "from beach_season as bs ",
           "inner join beach_boundary_history as bb on bs.beach_id = bb.beach_id ",
           "left join season_status_lut as ss on bs.season_status_id = ss.season_status_id ",
           "left join species_group_lut as sg on bs.species_group_id = sg.species_group_id ",
           "where date_part('year', bs.season_start_datetime) >= {current_year - offset_years} ",
           "and date_part('year', bs.season_start_datetime) <= {current_year} ",
           "and ss.season_status_code in ('OR', 'CR', 'OB', 'CB') ",
           "order by bb.beach_name, sg.species_group_code, bs.season_start_datetime")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach_season = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
beach_season = beach_season %>% 
  mutate(season_start = with_tz(season_start, tzone = "America/Los_Angeles")) %>% 
  mutate(season_end = with_tz(season_end, tzone = "America/Los_Angeles"))

# Inspect years in the seasons data
unique(as.integer(substr(beach_season$season_start, 1, 4)))

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
           "where date_part('year', t.low_tide_datetime) >= {current_year - offset_years} ",
           "and date_part('year', t.low_tide_datetime) <= {current_year}")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
tide = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
tide = tide %>% 
  mutate(tide_date = with_tz(tide_date, tzone = "America/Los_Angeles")) %>% 
  mutate(tide_date = format(tide_date))

# Inspect years in the seasons data
unique(as.integer(substr(beach_season$season_start, 1, 4)))

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

# Historical data for adjoining Birch Bay BIDNs overlap over years in some cases. 
# Need to do joins one year at a time. There is no beach_id or other beach identifier for flights

# Get flight data. Add bidn later using spatial join
qry = glue("select s.survey_datetime as survey_date, st.survey_type_description as survey_type, ",
           "se.event_datetime as count_time, ht.harvester_type_code as user_type, ",
           "se.harvester_count as uclam, se.survey_event_id, pl.geom as geometry ",
           "from survey as s left join survey_type_lut as st on s.survey_type_id = st.survey_type_id ",
           "left join survey_event as se on s.survey_id = se.survey_id ",
           "left join harvester_type_lut as ht on se.harvester_type_id = ht.harvester_type_id ",
           "left join point_location as pl on se.event_location_id = pl.point_location_id ",
           "where date_part('year', s.survey_datetime) >= {current_year - offset_years} ", 
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

# Inspect years in the flight data
unique(as.integer(substr(flight_st$count_time, 1, 4)))

# Check hours
sort(unique(substr(format(flight_st$survey_date), 12, 13))) # Should be ""
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
st_crs(beach_st)$epsg
st_crs(flt_st)$epsg

#=== 2008 ==============

# Pull one year of data from flight
flt_st_08 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2008)

# Pull out one year of data from beach_st
bch_st_08 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2008) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_08$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_08)
flt_st_08 = st_join(flt_st_08, bch_st_08)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_08)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_08 = flt_st_08 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2009 ==============

# Pull one year of data from flight
flt_st_09 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2009)

# Pull out one year of data from beach_st
bch_st_09 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2009) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_09$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_09)
flt_st_09 = st_join(flt_st_09, bch_st_09)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_09)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_09 = flt_st_09 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2010 ==============

# Pull one year of data from flight
flt_st_10 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2010)

# Pull out one year of data from beach_st
bch_st_10 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2010) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_10$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_10)
flt_st_10 = st_join(flt_st_10, bch_st_10)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_10)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_10 = flt_st_10 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2011 ==============

# Pull one year of data from flight
flt_st_11 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2011)

# Pull out one year of data from beach_st
bch_st_11 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2011) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_11$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_11)
flt_st_11 = st_join(flt_st_11, bch_st_11)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_11)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_11 = flt_st_11 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2012 ==============

# Pull one year of data from flight
flt_st_12 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2012)

# Pull out one year of data from beach_st
bch_st_12 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2012) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_12$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_12)
flt_st_12 = st_join(flt_st_12, bch_st_12)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_12)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_12 = flt_st_12 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2013 ==============

# Pull one year of data from flight
flt_st_13 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2013)

# Pull out one year of data from beach_st
bch_st_13 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2013) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_13$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_13)
flt_st_13 = st_join(flt_st_13, bch_st_13)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_13)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_13 = flt_st_13 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2014 ==============

# Pull one year of data from flight
flt_st_14 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2014)

# Pull out one year of data from beach_st
bch_st_14 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2014) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_14$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_14)
flt_st_14 = st_join(flt_st_14, bch_st_14)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_14)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_14 = flt_st_14 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2015 ==============

# Pull one year of data from flight
flt_st_15 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2015)

# Pull out one year of data from beach_st
bch_st_15 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2015) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_15$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_15)
flt_st_15 = st_join(flt_st_15, bch_st_15)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_15)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_15 = flt_st_15 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2016 ==============

# Pull one year of data from flight
flt_st_16 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2016)

# Pull out one year of data from beach_st
bch_st_16 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2016) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_16$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_16)
flt_st_16 = st_join(flt_st_16, bch_st_16)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_16)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_16 = flt_st_16 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2017 ==============

# Pull one year of data from flight
flt_st_17 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2017)

# Pull out one year of data from beach_st
bch_st_17 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2017) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_17$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_17)
flt_st_17 = st_join(flt_st_17, bch_st_17)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_17)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_17 = flt_st_17 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2018 ==============

# Pull one year of data from flight
flt_st_18 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2018)

# Pull out one year of data from beach_st
bch_st_18 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2018) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_18$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_18)
flt_st_18 = st_join(flt_st_18, bch_st_18)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_18)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_18 = flt_st_18 %>% 
  filter(beach_name == "Birch Bay SP")

#=== 2019 ==============

# Pull one year of data from flight
flt_st_19 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2019)

# Pull out one year of data from beach_st
bch_st_19 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2019) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_19$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_19)
flt_st_19 = st_join(flt_st_19, bch_st_19)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_19)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

#=== 2020 ==============

# Pull one year of data from flight
flt_st_20 = flt_st %>% 
  filter(substr(survey_date, 1, 4) == 2020)

# Pull out one year of data from beach_st
bch_st_20 = beach_st %>% 
  filter(substr(active_datetime, 1, 4) == 2020) %>% 
  select(beach_id, bidn, beach_name, geometry) 

# Check for duplicate beach_ids
any(duplicated(bch_st_20$beach_id))

# Do spatial join to add bidns and beach_id
n_flt = nrow(flt_st_20)
flt_st_20 = st_join(flt_st_20, bch_st_20)

# Warning if spatial join adds rows
if (!n_flt == nrow(flt_st_20)) {
  cat("\nWARNING: Spatial join created new rows. Do not pass go!\n\n")
} else {
  cat("\nNumber of rows are as expected. Ok to proceed.\n\n")
}

# Get just the Birch Bay data
flt_st_20 = flt_st_20 %>% 
  filter(beach_name == "Birch Bay SP")

# Combine
flt_st = rbind(flt_st_08, flt_st_09, flt_st_10, flt_st_11, flt_st_12,
               flt_st_13, flt_st_14, flt_st_15, flt_st_15, flt_st_16,
               flt_st_17, flt_st_18, flt_st_19, flt_st_20)

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
# Creel data ---- Change this year to minus offset years
# rather than 8 years since no creels in 2020 due to COVID?
# This can be adjusted if need be
#===========================================================

# Get creel data....need to join bidn by both year and beach_id to avoid duplicate rows
# There are fewer rows than in old program. Due to one row per species. Need to verify means by year.
qry = glue("select se.survey_event_id, s.survey_datetime as survey_date, st.survey_type_description as survey_type, ",
           "s.beach_id, se.event_number, se.event_datetime as creel_time, ",
           "ht.harvester_type_code as harvester_type, se.harvester_count as n_party, ",
           "sp.common_name as species, sh.shell_condition_code as shell_condition, ",
           "spe.species_count, spe.species_weight_gram as species_wt ",
           "from survey as s ",
           "left join survey_type_lut as st on s.survey_type_id = st.survey_type_id ",
           "left join beach as b on s.beach_id = b.beach_id ",
           "left join survey_event as se on s.survey_id = se.survey_id ",
           "left join harvester_type_lut as ht on se.harvester_type_id = ht.harvester_type_id ",
           "left join species_encounter as spe on se.survey_event_id = spe.survey_event_id ",
           "left join species_lut as sp on spe.species_id = sp.species_id ",
           "left join shell_condition_lut as sh on spe.shell_condition_id = sh.shell_condition_id ",
           "where date_part('year', s.survey_datetime) >= {current_year - offset_years} ", 
           "and date_part('year', s.survey_datetime) <= {current_year} ",
           "and b.local_beach_name = 'Birch Bay SP' ",
           "and st.survey_type_description = 'Creel survey, clam and oyster, catch per unit effort'")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
creel_data = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
creel_data = creel_data %>% 
  mutate(survey_date = with_tz(survey_date, tzone = "America/Los_Angeles")) %>% 
  mutate(creel_time = with_tz(creel_time, tzone = "America/Los_Angeles")) 

# Inspect years in the creel data
unique(as.integer(substr(creel_data$survey_date, 1, 4)))

# Get bidns and beach_names for creel data. Can't use distinct in creel query otherwise data are lost....
# but joining by survey_event_id works perfect....
qry = glue("select distinct se.survey_event_id, bb.beach_number as bidn, bb.beach_name ",
           "from survey_event as se inner join survey as s on se.survey_id = s.survey_id ",
           "inner join beach_boundary_history as bb on s.beach_id = bb.beach_id ",
           "inner join survey_type_lut as st on s.survey_type_id = st.survey_type_Id ",
           "where date_part('year', s.survey_datetime) >= {current_year - offset_years} ", 
           "and date_part('year', s.survey_datetime) <= {current_year} ",
           "and st.survey_type_description = 'Creel survey, clam and oyster, catch per unit effort'")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
creel_beaches = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check for dups...If dups present...may need to change beach_names in pgAdmin....Then save.
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
  
#===========================================================
# Egress and mean-use ----
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
unique(creel$harvester_type)
unique(creel$species)
unique(creel$shell_condition)

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

# Check all bidns....Only
sort(unique(creel$bidn))

# End beach_id check section -----------------------------------------------------
  
# Get counts
table(creel$species, useNA = "ifany")
table(creel$shell_condition, useNA = "ifany")

# Format creel data
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

#===========================================================
# Format Seasons data ---- All are open season by definition
# Birch Bay is always open year around. No reason to creel
# when beach is closed to PSP...and if so we assume that 
# cpue will not differ
#===========================================================

# Inspect some values
unique(beach_season$season_status_code)
unique(beach_season$species_group_code)
sort(unique(substr(beach_season$season_start, 1, 4)))

# Inspect remaining bidns
sort(unique(beach_season$bidn))

# Pull out seasons data
seas = beach_season %>% 
  filter(season_status_code %in% c("OR", "CR")) %>% 
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

# Join to years
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

# Combine with flt and delete ground counts 
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
# Calculate N observations for the entire set of pooled seasons
# Will be used to set lower bounds on estimate range in the report
#============================================================================

# Calculate number of observations and harvesters over the entire period
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

# Add one set of values to be able to use flight at sunset on 3/15/2008
# Need Seattle only entry further below....~ line 1790
add_tide = tibble(tide_date = c("2008-03-15 17:43:00", "2008-03-15 18:33:00"),
                  tide_station = c("Port Townsend", "Seattle"),
                  tide_time = c(1063L, 1113L),
                  tide_height = c(-0.46, -0.22),
                  tide_strata = c("LOW", "LOW"),
                  survey_date = c("2008-03-15", "2008-03-15"))

# Add new tide entry for corner-case. Tide Time was cumputed slightly differently 
# in Region 4 at that time than currently...so only an XPLUS strata in the morning
# currently qualifies...but it is reasonable to include the late count on a LOW.
tide = rbind(tide, add_tide)

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
rm(list=c("chk_delete", "chk_dup_strata", "dup_dates", 
          "flt", "flt_raw", "flts", "fltsr", "no_flt_crl_one", 
          "no_flt_crl_two", "tide_check", "tide"))

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

#============================================================================
# Output initial Birch Bay Season data to excel ----
#============================================================================

# # Output an initial birch bay season dataset for later storage in DB
# birch_psp_season_one = both_seas
# birch_psp_season_two = both_seas %>% 
#   mutate(crlsea = "CC")
# birch_psp_season = rbind(birch_psp_season_one, birch_psp_season_two) %>% 
#   arrange(year, begin, end) %>% 
#   mutate(beach_id = tolower(beach_id))
# 
# # Output with styling
# num_cols = ncol(birch_psp_season)
# out_name = paste0(2019L, "_", "birch_psp_season.xlsx")
# wb <- createWorkbook(out_name)
# addWorksheet(wb, "birch_psp_season", gridLines = TRUE)
# writeData(wb, sheet = 1, birch_psp_season, rowNames = FALSE)
# ## create and add a style to the column headers
# headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
#                            fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
# addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
# saveWorkbook(wb, out_name, overwrite = TRUE)

#============================================================================
# Import Birch Bay Season data from excel ----
#============================================================================

# # Import from xlsx...MUST ADD NEW VALUES EACH YEAR !!!!!!!!!!!!!!!!!!!!!!
# both_seas = read.xlsx("birch_psp_season.xlsx") %>% 
#   mutate(bidn = as.integer(bidn)) %>% 
#   mutate(year = as.integer(year))

# Import from seasons...MUST ADD NEW VALUES EACH YEAR !!!!!!!!!!!!!!!!!!!!!!
both_seas = beach_season %>% 
  mutate(year = as.integer(year(season_start))) %>% 
  filter(beach_name == "Birch Bay SP") %>% 
  #filter(species_group_code == "Clam") %>% 
  filter(season_status_code %in% c("OB", "CB")) %>% 
  mutate(begin = format(season_start)) %>% 
  mutate(end = format(season_end)) %>% 
  mutate(fltsea = case_when(
    season_status_code == "OB" ~ "OO",
    season_status_code == "CB" ~ "CC")) %>% 
  select(bidn, beach_name, year, begin, 
         end, fltsea) %>% 
  distinct() %>% 
  arrange(bidn, year, begin)

# Pull out seasons data for all years since effort pooled means are needed
flight_seas = both_seas %>% 
  # filter(year == current_year) %>% 
  select(bidn, beach_name, year, begin, end, fltsea)

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

#============================================================================
# Pull out just 2008-2012 for comparison with Alex's numbers
#============================================================================

# # Filter to only years 2008-12          !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  
# dsflt = dsflt %>% 
#   filter(year %in% c(2008, 2009, 2010, 2011, 2012))

# For each stratum and fltsea, compute mean and variance of daily effort, 
# sum of effort variance, and n sample days......CAN COMPUTE ALEX VALUES FROM HERE !!!!!!!!!!!!!!!!!!!!!!!!!
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

# Select the worst tide to delete
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
  mutate(vEh.mn = vEh1 + vEh2) %>% 
  mutate(se_est = sqrt(vEh.mn)) %>% 
  mutate(rse_est = se_est / Eh.mn) %>% 
  mutate(se_double = 2 * se_est) %>% 
  mutate(upper_ci = Eh.mn + se_double)

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
  summarize(Eh.mn = mean(ehatdh, na.rm = TRUE),
            vEh.mn = var(ehatdh, na.rm = TRUE),
            nEh.mn = n()) %>%
  ungroup() %>% 
  select(bidn, fltsea, Eh.mn, vEh.mn, nEh.mn)

# Combine plustides and Ehattot data....SHOULD "OC" be included in bottom condition ??????
plus_mean = teff %>% 
  left_join(plus_tides, by = c("bidn", "fltsea")) %>% 
  mutate(pEh.mn = case_when(
    fltsea == "OO" ~ 0.16 * Eh.mn,
    fltsea == "CC" ~ 0.08 * Eh.mn,
    !fltsea %in% c("OO", "CC") ~ 0.0)) %>% 
  mutate(var_est = vEh.mn / nEh.mn) %>% 
  mutate(se_est = sqrt(var_est)) %>% 
  mutate(rse_est = se_est / Eh.mn) %>% 
  mutate(upper_bound = (2 * rse_est) * pEh.mn) %>% 
  mutate(upper_ci = pEh.mn + upper_bound)

#==============================================================================
# COMPUTE TOTAL "YEAR-LONG" EFFORT BY BIDN AND FLTSEASON ----
#==============================================================================

# Combine veff and plus_mean
flt_mean = veff %>% 
  select(bidn, fltsea, tide_strata, Eh.mn, vEh.mn, se_double, upper_ci)

plus_mean = plus_mean %>% 
  mutate(tide_strata = "PLUS") %>% 
  select(bidn, fltsea, tide_strata, Eh.mn, vEh.mn, se_double = upper_bound, upper_ci)

# Combine
effort_mean = rbind(flt_mean, plus_mean)

# Add number of tides in each fltsea
# Trim to only count tides in 2019
tds_yr = tdsdh %>% 
  mutate(tide_year = as.integer(substr(tide_date, 1, 4))) %>% 
  filter(tide_year == current_year)

# Compute the number of available tide days by BIDN
tdsn_yr = tds_yr %>% 
  group_by(bidn, tide_strata, fltsea) %>% 
  summarize(Dh = n()) %>% 
  select(bidn, tide_strata, fltsea, Dh) %>% 
  ungroup()

# Join to effort_mean
ehatgt = effort_mean %>% 
  left_join(tdsn_yr, by = c("bidn", "tide_strata", "fltsea")) %>% 
  mutate(Ehatpw = upper_ci * Dh)

# Calculate full year effort
ehatsum = ehatgt %>% 
  group_by(bidn) %>% 
  summarize(total_effort = sum(Ehatpw, na.rm = TRUE)) %>% 
  ungroup()

#=========================================================================
# COMPUTE TOTAL "YEAR-LONG" HARVEST ON EACH BEACH BY FLTSEASON ----
#=========================================================================

# Pull out needed variables for next steps
ehatgt = ehatgt %>% 
  select(bidn, fltsea, Ehatpw) %>% 
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

# Trim cpu to only actively managed beaches
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

# Calculate catch for each species over the year
catch_year = catch %>% 
  group_by(bidn, species) %>% 
  summarize(total_catch = sum(season_catch, na.rm = TRUE)) %>% 
  select(bidn, species, total_catch)

# Join effort data that includes plus and winter effort with catch estimates
chatt_all = catch_year %>% 
  left_join(ehatsum, by = "bidn") %>% 
  select(bidn, effort = total_effort, species, catch = total_catch)

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
  spread(species_group, allowable_harvest)

# Pull out harvest numbers, one line per bidn
harv_effort = chatt_all %>% 
  select(bidn, effort) %>% 
  distinct()

# Pivot out catch numbers, one line per bidn
harv_catch = chatt_all %>% 
  mutate(catch = round(catch)) %>% 
  select(bidn, species, catch) %>% 
  spread(species, catch)
  
harvest = harv_effort %>% 
  left_join(harv_catch, by = "bidn") %>% 
  select(bidn, total_effort = effort, butter_catch = but, cockle_catch = coc, 
         eastern_catch = ess, geoduck_catch = geo, horse_catch = hor, 
         manila_catch = man, native_catch = nat, oyster_catch = oys)
  
# Join to bidnfo
harvest_rep = harvest %>% 
  left_join(allow, by = "bidn")

#============================================================================
# Get bidn data with sfma and mng_region ----
#============================================================================

# Add beach names
bch_info_st = bch_st_19

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
bch_center_st = st_as_sf(bch_center, coords = c("lon", "lat"), crs = 2927)

# # Convert back to state plane
# bch_center_st = st_transform(bch_center_st, 2927)

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
  mutate(remaining_manila = as.integer(Manila - manila_catch)) %>%
  mutate(beach_status = if_else(is.na(beach_status), "Passive", beach_status)) %>% 
  mutate(total_effort = round(total_effort)) %>% 
  select(BIDN = bidn, BeachName = beach_name, Status = beach_status,
         EstimateType = estimate_type, ReportType = report_type,
         MngReg = mng_region, SFMA = sfma, SportEffortEstimate = total_effort,
         ManilaClamEstimate = manila_catch, SportManilaAllowable = Manila, 
         RemainingManilaAllowable = remaining_manila, NativeLittleneckEstimate = native_catch,
         TotalLittleneckEstimate = sum_littleneck, ButterClamEstimate = butter_catch, 
         HorseClamEstimate = horse_catch, CockleEstimate = cockle_catch, 
         EasternSoftshellEstimate = eastern_catch, GeoduckEstimate = geoduck_catch, 
         OysterEstimate = oyster_catch)

# Calculate time for running program: 47.45288 secs
Sys.time() - strt

#============================================================================
# Output harvest report to excel ----
#============================================================================

# Get rid of NAs in excel output
harvest_report[] = lapply(harvest_report, set_empty)

# Output with styling
num_cols = ncol(harvest_report)
out_name = glue("Summary/HarvestAssessment/data/{current_year}_", "BirchBayHarvestReport.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "BirchBayHarvestReport", gridLines = TRUE)
writeData(wb, sheet = 1, harvest_report, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

#============================================================================
# Prepare data for the projection program ---- Only write mean effort
# mean_cpue is written using the main harvest program
#============================================================================

# Add beach info and arrange
mean_effort_est = effort_mean %>% 
  left_join(bidns, by = "bidn") %>% 
  left_join(tdsn_yr, by = c("bidn", "tide_strata", "fltsea"))
  
# Check far missing beach_id
any(is.na(mean_effort_est$beach_id))

# Add id and final fields
mean_effort_est = mean_effort_est %>% 
  mutate(mean_effort_estimate_id = remisc::get_uuid(nrow(mean_effort_est))) %>% 
  mutate(estimation_year = current_year) %>% 
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  arrange(beach_name, fltsea, tide_strata) %>% 
  select(mean_effort_estimate_id, beach_id, beach_number = bidn, beach_name,
         estimation_year, tide_strata, flight_season = fltsea, mean_effort = Eh.mn,
         tide_count = Dh, created_datetime, created_by, modified_datetime,
         modified_by)

# # Dump previously uploaded values....but just for Birch Bay ...standard practice each year
# qry = glue("delete from mean_effort_estimate ",
#            "where estimation_year = {current_year} ",
#            "and beach_name = 'Birch Bay SP'")
#
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#  
# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "mean_effort_estimate", mean_effort_est, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)
#
# # Write to shellfish
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "mean_effort_estimate", mean_effort_est, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)





