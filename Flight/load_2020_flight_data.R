#=================================================================
# Load 2020 flight data to shellfish database
#
# NOTES:
#  1. Load BIDN data first. Then create files for FlightProof program.
#  2. Need to enforce consistent naming and EPSG codes for GIS data.
#
#  ToDo:
# 1.
#
# AS 2020-10-29
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Libraries
library(dplyr)
library(DBI)
library(glue)
library(sf)
library(stringi)
library(lubridate)
library(openxlsx)
library(uuid)

# Keep connections pane from opening
options("connectionObserver" = NULL)

# Globals
current_year = 2020L

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

# Function to convert tide time to minutes
hm_to_min = function(x) {
  hr = as.integer(substr(x, 1, 2))
  hr = hr * 60
  mn = as.integer(substr(x, 4, 5))
  mins = hr + mn
  mins
}

# Function to convert time in minutes from midnight to h:m
min_to_hm = function(x) {
  h = floor(x / 60)
  m = x %% 60
  hm = paste0(h, ":", m)
  return(hm)
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

#==============================================================================
# Import beach data from shellfish DB
#==============================================================================

# Read beach data ====================================

# Import the beach data from shellfish DB
qry = glue::glue("select beach_id, beach_number as bidn, beach_name, active_datetime, ",
                 "inactive_datetime, geom AS geometry ",
                 "from beach_boundary_history")

db_con = pg_con_local(dbname = "shellfish")
beach_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Get start and end years for beaches
beach_st = beach_st %>%
  mutate(active_datetime = with_tz(active_datetime, tzone = "America/Los_Angeles")) %>%
  mutate(inactive_datetime = with_tz(inactive_datetime, tzone = "America/Los_Angeles")) %>%
  mutate(start_yr = year(active_datetime)) %>%
  mutate(end_yr = year(inactive_datetime))

# Inspect
sort(unique(beach_st$end_yr))
sort(unique(beach_st$inactive_datetime))

# Pull out separate dataset without geometry
beach = beach_st %>%
  st_drop_geometry()

# Read shellfish_management_area as geometry data ====================================

# Import from shellfish DB
qry = glue::glue("select shellfish_management_area_id, shellfish_area_code, ",
                 "geom AS geometry ",
                 "from shellfish_management_area_lut")

# Read
db_con = pg_con_local(dbname = "shellfish")
sfma_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Read management_region as geometry data ====================================

# Import from shellfish DB
qry = glue::glue("select management_region_id, management_region_code, ",
                 "geom AS geometry ",
                 "from management_region_lut")

# Read
db_con = pg_con_local(dbname = "shellfish")
mng_reg_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

#======================================================================================
# Output for FlightProof program
#======================================================================================

# # Output BIDN file to proofing folder...create folder first
# bidn_proof = beach_st %>%
#   mutate(active_year = year(active_datetime)) %>%
#   mutate(inactive_year = year(inactive_datetime)) %>%
#   filter(active_year == current_year & inactive_year == current_year) %>%
#   select(BIDN = bidn, name = beach_name) %>%
#   st_transform(., 4326)
# st_crs(bidn_proof)$epsg
# proof_path = glue("C:\\data\\intertidal\\Apps\\gis\\{current_year}\\")
# write_sf(bidn_proof, dsn = glue("{proof_path}\\BIDN_{current_year}.shp"), delete_layer = TRUE)

#======================================================================================
# Flight data
#======================================================================================

#==========================================================================================
# Get the flight count data. File holds counts > 0, obs_time and all other associated data
#==========================================================================================

# Get flight data
flt_obs = read_sf(glue("Flight/data/{current_year}_FlightCounts/UClam_{current_year}.shp"))

# Check CRS: 4152 in 2020
st_crs(flt_obs)$epsg

# Inspect
sort(unique(flt_obs$UClam))
unique(flt_obs$User_)
#any(is.na(flt_obs$Time))
any(is.na(flt_obs$TIME))
# unique(flt_obs$TIME)        # MUST BE "00:00" format !!!!! Otherwise get error below
# unique(flt_obs$Time)        # MUST BE "00:00" format !!!!! Otherwise get error below
# unique(flt_obs$Comments)
unique(flt_obs$DATE)
#unique(flt_obs$Date)
n_flt_dates = unique(flt_obs$DATE)
length(n_flt_dates)

# # Check if any Date_CH2 disagree with Date....All agree
# chk_date = flt_obs %>%
#   filter(!Date_CH2 == Date)

# Format
flt_obs = flt_obs %>%
  mutate(TIME = if_else(nchar(TIME) == 4, paste0("0", TIME), TIME)) %>%
  # mutate(Time = if_else(nchar(Time) == 4, paste0("0", Time), Time)) %>%
  mutate(uuid = get_uuid(nrow(flt_obs))) %>%
  # select(uuid, flt_date = Date, flt_bidn = BIDN, flt_beach_name = NAME,
  #        obs_time = Time, uclam = UClam, user = User_, comments = Comments)
  select(uuid, flt_date = DATE, flt_bidn = BIDN, flt_beach_name = name,
         obs_time = TIME, uclam = UClam, user = User_, comments = Comments)

# Check time again
# unique(flt_obs$obs_time)
unique(nchar(flt_obs$obs_time))

# Verify crs...need to standardize on mobile end
st_crs(flt_obs)$epsg

# Convert to proper crs
flt_obs = st_transform(flt_obs, 2927)

# Verify crs
st_crs(flt_obs)$epsg

# Verify how many bidns per date
chk_flt_dates = flt_obs %>%
  st_drop_geometry %>%
  select(flt_date, flt_bidn) %>%
  distinct() %>%
  group_by(flt_date) %>%
  tally()

# Get geometry data for spatial join
bchyr_st = beach_st %>%
  filter(start_yr <= current_year & end_yr >= current_year) %>%
  select(beach_id_st = beach_id, bidn_st = bidn, beach_name_st = beach_name,
         start_yr, end_yr, geometry)

# # Get geometry data for spatial join
# bchyr_st = beach_st %>%
#   filter(start_yr <= current_year & end_yr >= current_year) %>%
#   select(beach_id_st = beach_id, bidn_st = bidn, beach_name_st = beach_name,
#          start_yr, end_yr, geometry)

# Verify all are in current year
unique(bchyr_st$start_yr)
unique(bchyr_st$end_yr)

# Get rid of year values
bchyr_st = bchyr_st %>%
  select(-c(start_yr, end_yr))

# Warn for duplicated bidns
if (any(duplicated(bchyr_st$bidn_st))) {
  cat("\nWarning: Duplicated BIDN. Investigate!\n\n")
}

# # Identify second instance of Toandos State Park geom
# bchyr_st = bchyr_st %>%
#   group_by(bidn_st) %>%
#   mutate(n_seq = row_number(bidn_st)) %>%
#   ungroup()
#
# # Check if beach truly duplicated...not exactly
# chk_bch = bchyr_st %>%
#   filter(bidn_st == 270080)
# identical(chk_bch$geometry[1], chk_bch$geometry[2])
#
# # Get rid of second instance of Toandos State Park geom
# bchyr_st = bchyr_st %>%
#   filter(n_seq == 1) %>%
#   select(- n_seq)
#
# # Warn for duplicated bidns
# if (any(duplicated(bchyr_st$bidn_st))) {
#   cat("\nWarning: Duplicated BIDN. Investigate!\n\n")
# }

# Check crs
flt_st = flt_obs
st_crs(bchyr_st)$epsg
st_crs(flt_st)$epsg

# Check for missing geometry
any(is.na(flt_st$geometry))
all(st_is_valid(flt_st))

# Add BIDNs via spatial join
flt_st = st_join(flt_st, bchyr_st)

# Look for missing BIDNs....None this year
no_bidn_flt_obs = flt_st %>%
  filter(is.na(bidn_st)) %>%
  arrange(flt_date, obs_time) %>%
  select(uuid, flt_date, obs_time, uclam, user, flt_bidn, flt_beach_name, comments)

#======================================================================================
# Output for FlightProof program
#======================================================================================

# # Output flight data to proofing folder
# flt_proof = flt_st %>%
#   select(BIDN = bidn_st, name = beach_name_st, date = flt_date, time = obs_time,
#          uclam, user_ = user, comments) %>%
#   st_transform(., 4326)
# st_crs(flt_proof)$epsg
# proof_path = glue("C:\\data\\intertidal\\Apps\\gis\\{current_year}\\")
# write_sf(flt_proof, dsn = glue("{proof_path}\\UCLAM_{current_year}.shp"), delete_layer = TRUE)

#==========================================================================================
# Get the zero count data. File holds counts == 0
# Had to manually move zeros into BIDNs in 221 cases
#==========================================================================================

# # In 2019 was being imported as 4269, but was actually 2927
# # In 2020 was imported as 4326
flt_zero = read_sf(glue("Flight/data/{current_year}_FlightCounts/UClam_Zero_{current_year}.shp"))

# Verify crs
st_crs(flt_zero)$epsg

# Convert to proper crs
flt_zero = st_transform(flt_zero, 2927)

# Verify crs
st_crs(flt_zero)$epsg

# Inspect
sort(unique(flt_zero$uclam))
# sort(unique(flt_zero$Uclam))
unique(flt_zero$user_)
# unique(flt_zero$User_)
any(is.na(flt_zero$time))
#any(is.na(flt_zero$Time))
unique(flt_zero$time)        # MUST BE "0:00" !!!!! Otherwise get error below
# unique(flt_zero$Time)        # MUST BE "0:00" !!!!! Otherwise get error below
unique(flt_zero$date)
# unique(flt_zero$Date)
unique(nchar(flt_zero$date))
# unique(nchar(flt_zero$Date))
n_zero_dates = unique(flt_zero$date)
# n_zero_dates = unique(flt_zero$Date)
length(n_zero_dates)

# Check for dates in zero's not in flt or vice versa
all(n_flt_dates %in% n_zero_dates)
all(n_zero_dates %in% n_flt_dates)
n_zero_dates[!n_zero_dates %in% n_flt_dates]

# Compare visually
sort(n_zero_dates)
sort(n_flt_dates)

# What?...so check data types for this iteration
typeof(n_flt_dates)
typeof(n_zero_dates)

# Convert n_flt_dates to character
n_flt_dates = as.character(n_flt_dates)

# Check for dates in zero's not in flt or vice versa
all(n_flt_dates %in% n_zero_dates)
all(n_zero_dates %in% n_flt_dates)
# Check for zero dates not in flt_dates
n_zero_dates[!n_zero_dates %in% n_flt_dates]
# Check for flt_dates not in zero_dates
n_flt_dates[!n_flt_dates %in% n_zero_dates]

# REMEMBER TO CORRECT date type in flt data !!!!!!!!!!!!!!!!!!!!

# Check NA times
chk_time = flt_zero %>%
  filter(is.na(time))

# Check user
table(flt_zero$user_, useNA = "ifany")

# # Get rid of NA record for TIME....No useful coordinates
# flt_zero = flt_zero %>%
#   filter(!is.na(TIME))

# Format
flt_zero = flt_zero %>%
  mutate(flt_time = if_else(nchar(time) == 4, paste0("0", time), time)) %>%
  mutate(uuid = get_uuid(nrow(flt_zero))) %>%
  mutate(user_ = "S") %>%
  mutate(obs_type = "flight") %>%
  mutate(comments = NA_character_) %>%
  select(uuid, obs_type, flt_date = date, flt_bidn = BIDN, flt_beach_name = name,
         obs_time = time, uclam, user = user_, comments)

# Verify crs
st_crs(flt_zero)$epsg

# Verify how many bidns per date
chk_zero_dates = flt_zero %>%
  st_drop_geometry %>%
  select(flt_date, flt_bidn) %>%
  distinct() %>%
  group_by(flt_date) %>%
  tally()

# Check for missing geometry
any(is.na(flt_zero$geometry))
all(st_is_valid(flt_zero))

# Add BIDNs via spatial join
flt_zero = st_join(flt_zero, bchyr_st)

# Look for missing BIDNs....Only cases were for Terrel Cove and Cottonwood Park
no_bidn_zero_obs = flt_zero %>%
  filter(is.na(bidn_st)) %>%
  arrange(flt_date, obs_time) %>%
  select(uuid, flt_date, obs_time, uclam, user, flt_bidn, flt_beach_name, comments)

# #======================================================================================
# # Output for FlightProof program
# #======================================================================================

# # Output zero data to proofing folder
# zero_proof = flt_zero %>%
#   select(BIDN = bidn_st, name = beach_name_st, date = flt_date, time = obs_time,
#          uclam, user_ = user) %>%
#   st_transform(., 4326)
# st_crs(bidn_proof)$epsg
# proof_path = glue("C:\\data\\intertidal\\Apps\\gis\\{current_year}\\")
# write_sf(zero_proof, dsn = glue("{proof_path}\\ZERO_{current_year}.shp"), delete_layer = TRUE)

#=============================================================================
# Check for errant obs_time values in flt_obs
#=============================================================================

# Get tide times data
qry = glue("select distinct t.low_tide_datetime as tide_date, pl.location_name as tide_station, ",
           "t.tide_time_minutes as tide_time, t.tide_height_feet as tide_height, ",
           "ts.tide_strata_code as tide_strata ",
           "from tide as t ",
           "left join point_location as pl ",
           "on t.tide_station_location_id = pl.point_location_id ",
           "left join tide_strata_lut as ts ",
           "on t.tide_strata_id = ts.tide_strata_id ",
           "where date_part('Year', t.low_tide_datetime) = {current_year} ",
           "order by t.low_tide_datetime")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
tide_times = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
tide_times = tide_times %>%
  mutate(tide_date = with_tz(tide_date, tzone = "America/Los_Angeles")) %>%
  mutate(tide_date = format(tide_date))

# Check if any tide_times strata differ by date...should only be Seattle strata...All Ok
time_check = tide_times %>%
  select(tide_date, tide_strata) %>%
  distinct() %>%
  group_by(tide_date) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq > 1)

# Reduce down to only lowest tide per day
tides = tide_times %>%
  rename(low_tide_datetime = tide_date) %>%
  mutate(mnth = as.integer(month(low_tide_datetime))) %>%
  filter(mnth %in% seq(2, 9)) %>%
  filter(!tide_strata == "NIGHT") %>%
  mutate(tide_date = substr(format(low_tide_datetime), 1, 10)) %>%
  arrange(tide_date, tide_height) %>%
  mutate(tide_time_hm = min_to_hm(tide_time)) %>%
  mutate(flt_date = tide_date) %>%
  select(flt_date, tide_station, tide_time_hm, tide_time, tide_height, tide_strata)

# Filter down to only one tide per day
tides = tides %>%
  arrange(flt_date, desc(tide_station), tide_strata) %>%
  group_by(flt_date, tide_station) %>%
  mutate(n_seq = row_number()) %>%
  ungroup() %>%
  filter(n_seq == 1) %>%
  select(-n_seq)

# Check for duplicates
any(duplicated(tides$flt_date))

# Get the tide correction data
qry = glue("select distinct bb.beach_number as bidn, b.local_beach_name as beach_name, ",
           "pl.location_name as tide_station, b.low_tide_correction_minutes as lt_corr ",
           "from beach as b ",
           "inner join beach_boundary_history as bb ",
           "on b.beach_id = bb.beach_id ",
           "left join point_location as pl ",
           "on b.tide_station_location_id = pl.point_location_id ",
           "order by bb.beach_number")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
tide_corr = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check for duplicated BIDNs
chk_dup_beach = tide_corr %>%
  filter(duplicated(bidn)) %>%
  left_join(tide_corr, by = "bidn")

# Report if any duplicated beach_ids or BIDNs
if (nrow(chk_dup_beach) > 0) {
  cat("\nWARNING: Duplicated BIDNs. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated BIDNs. Ok to proceed.\n\n")
}

# Pull out count data for comparison with tides data
count_times = flt_st %>%
  st_drop_geometry() %>%
  mutate(data_source = "flt_counts") %>%
  select(flt_date, bidn = bidn_st, beach_name = flt_beach_name, obs_time, uclam,
         user, comments, data_source) %>%
  filter(!is.na(obs_time) & !obs_time == "00:00")

# Join obs_times and corrections
tide_times = count_times %>%
  select(-beach_name) %>%
  mutate(bidn = as.integer(bidn)) %>%
  left_join(tide_corr, by = "bidn")

# Join obs_times and tide_times
tide_times = tide_times %>%
  mutate(flt_date = as.character(flt_date)) %>%
  left_join(tides, by = c("flt_date", "tide_station")) %>%
  mutate(tide_min = tide_time + lt_corr) %>%
  # CAN GENERATE WARNING HERE IF NO OBS_TIME !!!!!!!!!!!!!!!
  mutate(obs_min = hm_to_min(obs_time)) %>%
  mutate(minutes_off = abs(tide_min - obs_min)) %>%
  arrange(desc(minutes_off))

# Check distribution
table(tide_times$minutes_off, useNA = "ifany")

# RESULT: Max dif was 85 minutes,
#         Typically times are off by an hour or more at the start
#         then dwindle to minimal as route heads southward

# Check where times differ
chk_count_times = tide_times %>%
  filter(minutes_off > 60) %>%
  select(flt_date, bidn, beach_name, ref_tide_time = tide_time, tide_station,
         tide_height, tide_strata, lt_corr, tide_min, obs_min, minutes_off)

#=============================================================================
# Prep to combine with other datasets
#=============================================================================

# Check for missing beach_id...None
any(is.na(flt_st$beach_id_st))

# Check user
unique(flt_st$user)

# Check beach_names and BIDNs
chk_beach_names = flt_st %>%
  st_drop_geometry() %>%
  mutate(flt_bidn = as.integer(flt_bidn)) %>%
  select(flt_bidn, bidn_st, flt_beach_name, beach_name_st) %>%
  distinct() %>%
  mutate(bidn_dif = if_else(!flt_bidn == bidn_st, "different", "the same"))

# Message to inspect
cat("\nMake sure to manually inspect 'chk_beach_names' to make sure names and BIDNs match!\n\n")

# Format flt_obs for binding
flt_obs = flt_st %>%
  mutate(user = "R") %>%
  mutate(comments = trim(comments)) %>%
  mutate(comments = if_else(comments == "", NA_character_, comments)) %>%
  mutate(source = "aerial") %>%
  select(flt_date, beach_id = beach_id_st, bidn = bidn_st, beach_name = beach_name_st,
         obs_time, uclam, user, comments, source)

#=============================================================================
# Add zero counts to flt_obs
#=============================================================================

# Format fz for binding
fz = flt_zero %>%
  mutate(user = "S") %>%
  mutate(comments = trim(comments)) %>%
  mutate(comments = if_else(comments == "", NA_character_, comments)) %>%
  mutate(source = "aerial") %>%
  select(flt_date, flt_bidn, flt_beach_name, obs_time, uclam, user,
         comments, source)

# Check crs prior to join
st_crs(fz)$epsg
st_crs(bchyr_st)$epsg

# Add BIDNs via spatial join
fz = st_join(fz, bchyr_st)

# Check for missing beach_ids: Result: None
chk_fz = fz %>%
  filter(is.na(beach_id_st)) %>%
  select(flt_bidn, flt_beach_name) %>%
  st_drop_geometry()

# Pull out variables in common with flt_obs
fz_obs = fz %>%
  mutate(uclam = as.integer(uclam)) %>%
  select(flt_date, beach_id = beach_id_st, bidn = bidn_st, beach_name = beach_name_st,
         obs_time, uclam, user, comments, source)

# Needed to change datatypes for flt_obs
flt_obs = flt_obs %>%
  mutate(flt_date = as.character(flt_date)) %>%
  mutate(uclam = as.integer(uclam))

# Combine data into one dataset
flt = rbind(flt_obs, fz_obs)

#=============================================================================
# Get the ltc data
#=============================================================================

# Get the LTC data
ltc_obs = read_sf(glue("Flight/data/{current_year}_FlightCounts/LTC_{current_year}.shp"))

# Inspect
sort(unique(ltc_obs$Uclam))
unique(ltc_obs$User_)
any(is.na(ltc_obs$Time))
unique(ltc_obs$Time)        # MUST BE "00:00" format !!!!! Otherwise get error below
# unique(ltc_obs$Comments)
unique(ltc_obs$Date)
unique(ltc_obs$obs_type)
n_ltc_dates = unique(ltc_obs$Date)
length(n_ltc_dates)

# Format
ltc_obs = ltc_obs %>%
  mutate(uuid = remisc::get_uuid(nrow(ltc_obs))) %>%
  mutate(Time = if_else(nchar(Time) == 4, paste0("0", Time), Time)) %>%
  select(uuid, flt_date = Date, flt_bidn = BIDN, flt_beach_name = name,
         obs_type, obs_time = Time, uclam = Uclam, user = User_,
         comments = Comments)

# Check again
unique(ltc_obs$obs_time)
all(nchar(ltc_obs$obs_time) == 5L)

# Verify crs
st_crs(ltc_obs)$epsg

# Convert to proper crs
ltc_obs = st_transform(ltc_obs, 2927)

# Verify crs
st_crs(ltc_obs)$epsg

# Add source
ltc = ltc_obs %>%
  mutate(source = "ground") %>%
  select(uuid, flt_date, flt_bidn, flt_beach_name, obs_type, obs_time, uclam,
         user, comments, source)

# Format user
table(ltc$user, useNA = "ifany")
ltc = ltc %>%
  mutate(user = case_when(
    user == "Tribal" ~ "T",
    user == "Rec" ~ "R",
    user == "R" ~ "R"))
table(ltc$user, useNA = "ifany")

# Add BIDNs via spatial join
ltc_st = st_join(ltc, bchyr_st)

# Look for missing BIDNs
no_bidn_ltc_obs = ltc_st %>%
  filter(is.na(bidn_st)) %>%
  arrange(flt_date, obs_time) %>%
  select(uuid, flt_date, obs_type, obs_time, uclam, user, flt_bidn, flt_beach_name, comments)

# # Output ltc data to proofing folder
# ltc_proof = ltc_st %>%
#   select(BIDN = bidn_st, name = beach_name_st, date = flt_date, time = obs_time,
#          uclam, user_ = user, obs_type, comments) %>%
#   st_transform(., 4326)
# st_crs(ltc_proof)$epsg
# proof_path = glue("C:\\data\\intertidal\\Apps\\gis\\{current_year}\\")
# write_sf(ltc_proof, dsn = glue("{proof_path}\\LTC_{current_year}.shp"), delete_layer = TRUE)

#=============================================================================
# Check for errant obs_time values in ltc_obs
#=============================================================================

# # Filter out Dash Point (2019)...no polygon for beach. Not in plans.
# ltc_st = ltc_st %>%
#   filter(!flt_beach_name == "DASH POINT")

# Join tide_correction data to ltc_st
ltc_st_chk = ltc_st %>%
  mutate(bidn = as.integer(flt_bidn)) %>%
  mutate(flt_date = as.character(flt_date)) %>%
  left_join(tide_corr, by = "bidn") %>%
  left_join(tides, by = c("flt_date", "tide_station")) %>%
  mutate(tide_min = tide_time + lt_corr) %>%
  # CAN GENERATE WARNING HERE IF NO OBS_TIME !!!!!!!!!!!!!!!
  mutate(obs_min = hm_to_min(obs_time)) %>%
  mutate(minutes_off = abs(tide_min - obs_min)) %>%
  arrange(desc(minutes_off))

# Check distribution
table(ltc_st_chk$minutes_off, useNA = "ifany")

# RESULT: None greater than 62 minutes

# Double-check user
table(ltc_st$user, useNA = "ifany")

# Pull out variables in common with flt_obs
ltc_obs = ltc_st %>%
  mutate(source = "ground") %>%
  select(flt_date, beach_id = beach_id_st, bidn = bidn_st, beach_name = beach_name_st,
         obs_time, uclam, user, comments, source)

# Check for missing beach_ids
any(is.na(ltc_obs$beach_id))

#============================================================================================
# Process annual flight data for upload. ONLY UPLOAD AFTER DATA RUN THROUGH FlightProof !
#============================================================================================

# Check crs
st_crs(flt)$epsg
st_crs(ltc_st)$epsg

# Update datatypes as needed before binding
ltc_obs = ltc_obs %>%
  mutate(flt_date = as.character(flt_date)) %>%
  mutate(uclam = as.integer(uclam))

# Combine flt and ltc data
flt = rbind(flt, ltc_obs)

# Check flight data. Convert to st geometry. Will not work unless all coordinates present
chk_flt = st_transform(flt, crs = 4326)

# Pull out original lat-lons
chk_flt = chk_flt %>%
  mutate(lon = as.numeric(st_coordinates(geometry)[,1])) %>%
  mutate(lat = as.numeric(st_coordinates(geometry)[,2]))

# Check for any outlier coordinates
sort(unique(round(chk_flt$lat, 1)))
sort(unique(round(chk_flt$lon, 1)))

# Check dates
sort(unique(substr(flt$flt_date, 1, 4)))  # Year
sort(unique(substr(flt$flt_date, 6, 7)))  # Months

# Check obs_times
sort(unique(substr(flt$obs_time, 1, 2)))  # Hour
sort(unique(substr(flt$obs_time, 4, 5)))  # Minute

#======================================================================
# Process annual flight data for upload
#======================================================================

# Locate cases where bidn > 0 and beach_id is missing
# None in 2020
chk_bidn = flt %>%
  filter(is.na(beach_id))

# # Output chk_bidn as a shape file
# write_sf(chk_bidn, dsn = glue("Shapefiles\\FlightCounts_{current_year}\\chk_bidn.shp"), delete_layer = TRUE)

# survey table ===========================================================================

# Define LTC vs Flight survey_type =========

# Check for missing date or geometry
any(is.na(flt$flt_date))
any(is.na(flt$geometry))

# Check varieties of User. flight surveys and LTCs are separate survey types.
unique(flt$user)

# Verify all entries in flt have a source
table(flt$source, useNA = "ifany")

# Define survey_type
flt = flt %>%
  mutate(ltc = if_else(source == "ground", "yes", "no"))

# Check
unique(flt$user)
(g = table(flt$user, useNA = "ifany"))

# Check
unique(flt$ltc)
(g2 = table(flt$ltc, useNA = "ifany"))

# Define the survey type
flt = flt %>%
  mutate(survey_type_id = if_else(ltc == "yes",
                                  "40b60612-2425-46b3-a5d5-c7fa9b4b0571",
                                  "7153c1cb-79cf-41d6-9fc1-966470c1460b"))

# Warn if survey_type_id missing
if (any(is.na(flt$survey_type_id))) {
  cat("\nWARNING: survey_type_id missing. Do not pass go!\n\n")
} else {
  cat("\nAll survey_type_ids defined. Ok to proceed.\n\n")
}

#==============================================================================================
# Need to separate LTCs from flight surveys. Each LTC date and beach combo is a separate survey
#==============================================================================================

# Verify no missing date
if (any(is.na(flt$flt_date))) {
  cat("\nWARNING: Date missing somewhere. Do not pass go!\n\n")
} else {
  cat("\nNo missing Dates. Ok to proceed.\n\n")
}

# Get ltc data
ltc_tab = flt %>%
  filter(ltc == "yes")

# Get flight data
flt_tab = flt %>%
  filter(ltc == "no")

#========== LTC data ================================

# Look for cases when two or more zero counts entered for same date, and beach combo
ltc_zero = ltc_tab %>%
  filter(uclam == 0L)

# Add a count by bidn, date, and user
ltc_zero = ltc_zero %>%
  group_by(bidn, flt_date, user) %>%
  mutate(n_zero = row_number(uclam)) %>%
  ungroup() %>%
  filter(n_zero == 1L) %>%
  select(-n_zero)

ltc_count = ltc_tab %>%
  filter(uclam > 0L)

# Combine back together
ltc_tab = rbind(ltc_count, ltc_zero)

# Generate the survey_id for LTCs
ltc_tab = ltc_tab %>%
  group_by(flt_date, bidn) %>%
  mutate(survey_id = remisc::get_uuid(1L)) %>%
  ungroup()

# Drop the geometry column for the survey table
surv_ltc = ltc_tab %>%
  select(survey_id, survey_type_id, beach_id,
         survey_datetime = flt_date)
surv_ltc$geometry = NULL

# Set to unique. Won't work with geometry column still in place.
# Also need beach_id for LTC data...but not with flight data
# For LTC one beach = one survey. For flights one survey = many beaches
survey_ltc = surv_ltc %>%
  distinct()

# Check for duplicated survey_ids
any(duplicated(survey_ltc$survey_id))

#========== Flight data ================================

# Look for cases when two or more zero counts entered for same date, and beach combo
flt_zero = flt_tab %>%
  filter(uclam == 0L)

# Add a count by bidn, date, and user
flt_zero = flt_zero %>%
  group_by(bidn, flt_date, user) %>%
  mutate(n_zero = row_number(uclam)) %>%
  ungroup() %>%
  filter(n_zero == 1L) %>%
  select(-n_zero)

flt_count = flt_tab %>%
  filter(uclam > 0L)

# Combine back together
flt_tab = rbind(flt_count, flt_zero)

# Generate the survey_id for flights
flt_tab = flt_tab %>%
  group_by(flt_date) %>%
  mutate(survey_id = remisc::get_uuid(1L)) %>%
  ungroup() %>%
  filter(!is.na(beach_id))

# Drop the geometry column for the survey table
surv_flt = flt_tab %>%
  select(survey_id, survey_type_id, survey_datetime = flt_date)
surv_flt$geometry = NULL

# Set to unique. Won't work with geometry column still in place.
survey_flt = surv_flt %>%
  distinct()

# Check for duplicated survey_ids
any(duplicated(survey_ltc$survey_id))

# Check for duplicated survey_ids
any(duplicated(survey_flt$survey_id))

# Combine flt and ltc into one dataset for later tables
fltall = rbind(flt_tab, ltc_tab)

# Check on zero counts and duplicates over both flt and ltc data ================================

# Get data
chk_zero = fltall %>%
  select(survey_id, flt_date, obs_time, bidn, uclam, user)

# Convert to lat-lon
chk_zero = st_transform(chk_zero, 4326)

chk_zero = chk_zero %>%
  mutate(lon = as.numeric(st_coordinates(geometry)[,1])) %>%
  mutate(lat = as.numeric(st_coordinates(geometry)[,2])) %>%
  mutate(coords = paste0(lat, ":", lon)) %>%
  select(survey_id, flt_date, obs_time, bidn, uclam,
         user, coords)
chk_zero$geometry = NULL

# Make sure there is no more than one case per beach and day of a zero count
chk_zero = chk_zero %>%
  filter(uclam == 0L) %>%
  group_by(survey_id, flt_date, bidn) %>%
  mutate(n_zero = row_number(uclam)) %>%
  ungroup()

# Get only cases where n_zero > 1: None in 2019
chk_nzero = chk_zero %>%
  filter(n_zero > 1)

# Any LTC counts with more than one zero? YES for 2020.....FIX below.
table(chk_nzero$user, useNA = "ifany")

# Any cases where uclam is zero and multiple zeros entered where bidn > 0. None
table(chk_nzero$bidn, useNA = "ifany")

# Any duplicated coords by survey_id: Result....None. So they were entered separately.
chk_coords = chk_zero %>%
  group_by(survey_id, bidn, coords) %>%
  mutate(n_dups = row_number(uclam)) %>%
  ungroup() %>%
  filter(n_dups > 1L)

# Make sure there is no more than one case per beach and day of a zero count
# There were four in 2020....Let flight bio determine what happened. Could be
# wrong date, or some other data entry error.
chk_zero = chk_zero %>%
  filter(n_zero > 1)

# Message
if ( nrow(chk_zero) > 0L ) {
  cat("\nWARNING: Some zero counts entered more than once. Do not pass go!\n\n")
} else {
  cat("\nAll zero counts entered only once. Ok to proceed!\n\n")
}

# Load survey data ===========================================

# Add beach_id to survey_flt so datasets can be combined
survey_flt = survey_flt %>%
  mutate(beach_id = NA_character_) %>%
  select(survey_id, survey_type_id, beach_id, survey_datetime)

# Combine survey_flt and survey_ltc into one dataset
survey = rbind(survey_flt, survey_ltc)

# Check for any duplicated survey_ids
any(duplicated(survey$survey_id))

# Add remaining fields
survey = survey %>%
  mutate(survey_datetime = with_tz(as.POSIXct(survey_datetime, tz = "America/Los_Angeles"), tzone = "UTC")) %>%
  mutate(sampling_program_id = "f1de1d54-d750-449f-a700-dc33ebec04c6") %>%
  mutate(area_surveyed_id = "72e4eeea-ad0e-45d9-a2c9-3e2a5a5d06b7") %>%  # Defaulting to "Entire beach" correct the few incorrect later.
  mutate(data_review_status_id = "bdefcb1f-80c4-4921-9cf4-66c8dde02d4b") %>% # Defaulting to "Final"
  mutate(survey_completion_status_id = "d192b32e-0e4f-4719-9c9c-dec6593b1977") %>% # Defaulting to "Completed"
  mutate(point_location_id = NA_character_) %>%
  mutate(start_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(end_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(comment_text = NA_character_) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(survey_id, survey_type_id, sampling_program_id, beach_id, point_location_id,
         area_surveyed_id, data_review_status_id, survey_completion_status_id,
         survey_datetime, start_datetime, end_datetime, comment_text,
         created_datetime, created_by, modified_datetime, modified_by)

# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "survey", survey, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)
#
# # Write to shellfish on prod
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "survey", survey, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)

# Clean-up
rm(list = c("chk_bidn", "chk_fz", "flt", "flt_count", "flt_obs", "flt_tab",
            "flt_zero", "fz", "fz_obs", "ltc_tab", "ltc", "surv_flt",
            "surv_ltc", "survey", "survey_flt", "survey_ltc", "ltc_zero",
            "ltc_count"))

# survey_event table =================================================================

# Generate the survey_event_id
flt = fltall %>%
  mutate(survey_event_id = get_uuid(nrow(fltall))) %>%
  arrange(bidn, flt_date, obs_time)

# Check for duplicate survey_event_ids. There should no duplicates.
# Each observation should be unique.
if (any(duplicated(flt$survey_event_id))) {
  cat("\nWARNING: Some duplicated survey_event_ids. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated survey_event_ids. Ok to proceed.\n\n")
}

#========== Point location table ======================

# Since every row is a unique observation, generate UUID in flt
flt = flt %>%
  mutate(point_location_id = remisc::get_uuid(nrow(flt)))

# Pull out needed columns
pt_loc = flt %>%
  select(point_location_id, beach_id)

# Spatial join to get shellfish_management_area_id
# Verify both sides of st_join will be sf objects
inherits(pt_loc, "sf")
inherits(sfma_st, "sf")
inherits(mng_reg_st, "sf")
st_crs(pt_loc)$epsg
st_crs(sfma_st)$epsg
st_crs(mng_reg_st)$epsg

# Add sfma and mng_area via spatial join
pt_loc = st_join(pt_loc, sfma_st)
pt_loc = st_join(pt_loc, mng_reg_st)

# Check for missing values of sfma and mng_reg
any(is.na(pt_loc$shellfish_management_area_id))
any(is.na(pt_loc$management_region_id))
any(is.na(pt_loc$point_location_id))
any(duplicated(pt_loc$point_location_id))

# Get most recent gid
qry = glue("select max(gid) from point_location")

# Get values from shellfish
db_con = dbConnect(odbc::odbc(), dsn = "local_shellfish", timezone = "UTC")
max_gid = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Add one
new_gid = max_gid$max + 1L

# Add some columns
pt_loc_tab = pt_loc %>%
  mutate(location_type_id = "cd9b431e-4750-4f54-a67e-a4252a4189f2") %>%  # Intertidal harvest count
  mutate(beach_id = NA_character_) %>%
  mutate(location_code = NA_character_) %>%
  mutate(location_name = NA_character_) %>%
  mutate(location_description = NA_character_) %>%
  mutate(horizontal_accuracy = NA_real_) %>%
  mutate(comment_text = NA_character_) %>%
  mutate(gid = seq(new_gid, new_gid + nrow(pt_loc) - 1L)) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(point_location_id, location_type_id, beach_id,
         shellfish_management_area_id, management_region_id, location_code,
         location_name, location_description, horizontal_accuracy,
         comment_text, gid, geometry, created_datetime,
         created_by, modified_datetime, modified_by)

# # Write beach_history_temp to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# st_write(obj = pt_loc_tab, dsn = db_con, layer = "point_location_temp")
# DBI::dbDisconnect(db_con)
#
# # Write beach_history_temp to shellfish
# db_con = pg_con_prod(dbname = "shellfish")
# st_write(obj = pt_loc_tab, dsn = db_con, layer = "point_location_temp")
# DBI::dbDisconnect(db_con)

# Use select into query to get data into point_location
qry = glue::glue("INSERT INTO point_location ",
                 "SELECT CAST(point_location_id AS UUID), CAST(location_type_id AS UUID), ",
                 "CAST(beach_id AS UUID), location_code, location_name, ",
                 "location_description, horizontal_accuracy, comment_text, gid, ",
                 "geometry as geom, CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM point_location_temp")

# # Insert select to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # Insert select to shellfish
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# DBI::dbDisconnect(db_con)
#
# # Drop temp
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbExecute(db_con, "DROP TABLE point_location_temp")
# DBI::dbDisconnect(db_con)
#
# # Drop temp
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbExecute(db_con, "DROP TABLE point_location_temp")
# DBI::dbDisconnect(db_con)

#========== Back to survey_event table ======================

# # Write flt to a temp file
# flight_data = flt
# flt_dat = flt
# saveRDS(flt_dat, "flt_dat.rds")
# flt = readRDS("flt_dat.rds")
# identical(flt_dat, flt)

# Pull out survey_event data
survey_event = flt %>%
  select(survey_event_id, survey_id, event_location_id = point_location_id,
         bidn, beach_name, user, event_time = obs_time, event_date = flt_date,
         harvester_count = uclam, comments)
survey_event$geometry = NULL

# Verify no duplicated survey_event_ids
if (any(duplicated(survey_event$survey_event_id))) {
  cat("\nWARNING: Duplicated survey_event_id. Do not pass go!\n\n")
} else {
  cat("\nNo duplicated survey_event_ids. Ok to proceed.\n\n")
}

# No need for event number:
# Zero's and no time values for zero entries make that impossible for now.

# Check unique event_time
unique(survey_event$event_time)
any(!nchar(survey_event$event_time) == 5)

# Convert any time values where nchar == 5 to add leading zero
survey_event = survey_event %>%
  mutate(event_time = if_else(nchar(event_time) == 4,
                              paste0("0", event_time), event_time))

# Check agian
unique(survey_event$event_time)
any(!nchar(survey_event$event_time) == 5)

# Generate event_datetime
survey_event = survey_event %>%
  mutate(event_date = paste0(event_date, " ", event_time, ":00")) %>%
  mutate(event_datetime = with_tz(as.POSIXct(event_date, tz = "America/Los_Angeles"), tzone = "UTC"))

# Set harvester_type_id
unique(survey_event$user)
survey_event = survey_event %>%
  mutate(harvester_type_id = recode(user,
                                    "S" = "772f21fb-d07c-4998-a1d9-f7bb49df4db4",
                                    "R" = "772f21fb-d07c-4998-a1d9-f7bb49df4db4",
                                    "T" = "070980cf-74a7-4192-9319-f93325c7b6e4"))

# Add remaining fields
survey_event = survey_event %>%
  mutate(harvest_method_id = "68cb2cb1-77df-49f3-872b-5e3ba3299e14") %>%          # NA
  mutate(harvest_gear_type_id = "5d1e6be6-cd85-498c-b2fa-43b370d951b4") %>%       # NA
  mutate(harvest_depth_range_id = "b27281f7-9387-4a51-b91f-95748676f918") %>%     # NA
  mutate(event_number = NA_integer_) %>%
  mutate(harvest_gear_count = NA_integer_) %>%
  mutate(harvester_zip_code = NA_integer_) %>%
  mutate(comment_text = trimws(comments)) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(survey_event_id, survey_id, event_location_id, harvester_type_id,
         harvest_method_id, harvest_gear_type_id, harvest_depth_range_id,
         event_number, event_datetime, harvester_count, harvest_gear_count,
         harvester_zip_code, comment_text, created_datetime, created_by,
         modified_datetime, modified_by)

# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "survey_event", survey_event, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)
#
# # Write to shellfish on prod
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "survey_event", survey_event, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)

#============================================================================================
# Final check to verify the same number of rows exist in both local and production DBs
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



