#=================================================================
# Load new set of 2020 values to beach related tables
#
# NOTES:
#  1. Data should be pre-proofed by person responsible for all
#     flight and low-tide counts. The data can be proofed using
#     The FlightProof program, which currently it relies on import of
#     shapefiles. I have encouraged using a single file geodatabase
#     instead. Once that transition has been made the FlightProof
#     program can be amended accordingly. Should be an easy edit.
#
# All loaded on 2020-10-
#
# AS 2020-10-23
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Libraries
library(dplyr)
library(DBI)
library(RPostgres)
library(glue)
library(sf)
library(lubridate)
library(openxlsx)
library(stringi)
library(uuid)

# Set options
options(digits=14)

# Keep connections pane from opening
options("connectionObserver" = NULL)

# Global values
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

# #==============================================================================
# # Delete previously uploaded current year beach data in case of errors
# #==============================================================================
#
# # Get rid of all beach_boundary_history polygons for current year that were previously loaded
# qry = glue::glue("DELETE FROM beach_boundary_history ",
#                  "where created_datetime = '2019-10-31 22:12:29+00'")
#
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)
#
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)
#
# # Get rid of all beach table entries for current year that were previously loaded
# qry = glue::glue("DELETE FROM beach ",
#                  "where created_datetime = '2019-10-31 22:12:29+00'")
#
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)
#
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbExecute(db_con, qry)
# dbDisconnect(db_con)

#============================================================================================
# Verify the same number of rows exist in relevant tables in both local and production
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

#============================================================================================
# Get beach data
#============================================================================================

# Get existing beach data from DB...No BIDNs
db_con = pg_con_local(dbname = "shellfish")
bch = DBI::dbReadTable(db_con, "beach")
dbDisconnect(db_con)

# Get corresponding bidns from beach_history
qry = glue::glue("SELECT distinct beach_id, beach_number AS bidn, beach_name ",
                 "FROM beach_boundary_history")

db_con = pg_con_local(dbname = "shellfish")
bch_bidn = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Add bidns to bch
bch_bidn = bch_bidn %>%
  left_join(bch, by = "beach_id")

# Load the flight bidn geometry data for the current year
flt_raw = read_sf(glue("Beach/data/{current_year}_FlightBIDN/BIDN_{current_year}.shp"))

# Check for duplicated BIDNs
any(duplicated(flt_raw$BIDN))

# # Identify duplicates
# # Deleted a number of 2018 duplicates directly in the QGIS attribute table
# dup_raw = flt_raw %>%
#   filter(duplicated(BIDN))

# Pull out needed data from flight BIDN layer
flt = flt_raw %>%
  select(bidn = BIDN, beach_name = name, geometry)

# Check class and epsg. EPSG must be 4326 or 3857
class(flt$geometry)
st_crs(flt)$epsg

# Set crs to 2927
flt = st_transform(flt, crs = 2927)
st_crs(flt)$epsg

# Pull out only needed variables to join and add beach_id
bch_bidn = bch_bidn %>%
  select(beach_id, bidn, local_beach_name) %>%
  distinct()

# Check for duplicated bidns
any(duplicated(bch_bidn$bidn))

# Add beach_id from database to flt_beach
flt_beach = flt %>%
  left_join(bch_bidn, by = "bidn") %>%
  arrange(bidn)

#=======================================================================================
# Pull in flight_counts, ltc_counts, and zeros to verify all needed beaches are present
#=======================================================================================

# Load the counts data
flt_counts = read_sf(glue("Flight/data/{current_year}_FlightCounts/UClam_{current_year}.shp"))
flt_zeros = read_sf(glue("Flight/data/{current_year}_FlightCounts/UClam_Zero_{current_year}.shp"))
ltc_counts = read_sf(glue("Flight/data/{current_year}_FlightCounts/LTC_{current_year}.shp"))

# Pull out the BIDNs
fltc_ids = unique(flt_counts$BIDN)
fltz_ids = as.character(unique(flt_zeros$BIDN))
lct_ids = as.character(unique((ltc_counts$BIDN)))
all_bidns = unique(c(fltc_ids, fltz_ids, lct_ids))
all_bidns = all_bidns[!is.na(all_bidns) & !all_bidns == "0"]

# Identify any bidns in count data that are not in beach data
missing_bidn = all_bidns[!all_bidns %in% flt_beach$bidn]

# Display message regarding missing beaches
if ( length(missing_bidn) > 0 ) {
  cat("\nWARNING: Some beaches surveyed are not in Beach tables. Need to add beach(es) to DB!\n\n")
  # Determine where the two bidns that are not in bidn polygons occur in count data
  print(missing_bidn %in% flt_counts$BIDN)
  print(missing_bidn %in% as.character(flt_zeros$BIDN))
  print(missing_bidn %in% as.character(ltc_counts$BIDN))
  missing_bidn
} else {
  cat("\nAll needed beaches are in database. Ok to proceed.\n\n")
}

# Identify locations of missing beaches....
# RESULT: For 2019 March Point should be 221290, and LTC Count at Browns Point should be deleted
#         No need for any additional BIDNs. Inspected the layers in QGIS
chk_zeros = flt_zeros %>%
  filter(BIDN %in% as.integer(missing_bidn))

chk_ltc = ltc_counts %>%
  filter(BIDN %in% as.integer(missing_bidn))

#=======================================================================================
# Join full set of flight data to flt_beach to determine missing beaches
#=======================================================================================

# FOR 2019 all beaches except 200051 and 200053 were in the beach layer. Roy and Camille
# confirmed we can omit those two sets of four zero observations
# IF NEEDED SEE CODE FOR 2017...Will need entries beach table in addition to polygons

#==================================================================================
# Check for invalid polygons
#==================================================================================

# Pull out needed values from flt_beach
flt_beach = flt_beach %>%
  select(beach_id, local_beach_name, bidn, geometry)

# Check if geometry is valid
all(st_is_valid(flt_beach))

# Check for valid geometry
chk_geometry = flt_beach %>%
  mutate(valid_flag = st_is_valid(geometry)) %>%
  filter(valid_flag == FALSE)

# Warn if any invalid
if (nrow(chk_geometry) > 0) {
  cat("\nWARNING: At least one polygon invalid. Do not pass go!\n\n")
  chk_geometry
} else {
  cat("\nAll polygons are valid. Ok to proceed.\n\n")
}

# Identify where geometry is invalid
st_is_valid(chk_geometry$geometry, reason = TRUE)[[1]]
chk_geometry$beach_name[[1]]

#==================================================================================
# Prepare current_year beach polygons so they can be written to the DB
#==================================================================================

# Get most recent gid
qry = glue("select max(gid) from beach_boundary_history")

# Get values from shellfish
db_con = pg_con_local(dbname = "shellfish")
max_gid = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Add one
new_gid = max_gid$max + 1L

# Fill in names where missing
new_flt_beach = flt_beach %>%
  select(beach_id, beach_name = local_beach_name, bidn, geometry) %>%
  mutate(gid = seq(new_gid, new_gid + nrow(flt_beach) - 1L)) %>%
  mutate(beach_boundary_history_id = get_uuid(nrow(flt_beach))) %>%
  # Survey type hard-coded as Aerial harvester count
  mutate(survey_type_id = "7153c1cb-79cf-41d6-9fc1-966470c1460b") %>%
  mutate(active_indicator = 0L) %>%
  mutate(active_datetime = with_tz(as.POSIXct(glue("{current_year}-01-01"), tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(inactive_datetime = with_tz(as.POSIXct(glue("{current_year}-12-31"), tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(inactive_reason = NA_character_) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(beach_boundary_history_id, beach_id, survey_type_id, beach_number = bidn,
         beach_name, active_indicator, active_datetime, inactive_datetime,
         inactive_reason, gid, geometry, created_datetime, created_by, modified_datetime,
         modified_by)

# Check crs flt_beach
st_crs(new_flt_beach)$epsg

# # Last fix of the names...Not needed this year...all beach names are taken from beach table
# new_flt_beach = new_flt_beach %>%
#   mutate(beach_name = if_else(beach_name == "W.r. Hicks CP", "W.R. Hicks CP", beach_name)) %>%
#   mutate(beach_name = if_else(beach_name == "Sse Dabob Bay", "SSE Dabob Bay", beach_name)) %>%
#   mutate(beach_name = if_else(beach_name == "Sne Dabob Bay", "SNE Dabob Bay", beach_name))

# Write beach_history_temp to local shellfish
db_con = pg_con_local(dbname = "shellfish")
st_write(obj = new_flt_beach, dsn = db_con, layer = "beach_history_temp", overwrite = TRUE)
DBI::dbDisconnect(db_con)

# Write beach_history_temp to prod shellfish
db_con = pg_con_prod(dbname = "shellfish")
st_write(obj = new_flt_beach, dsn = db_con, layer = "beach_history_temp", overwrite = TRUE)
DBI::dbDisconnect(db_con)

# Use select into query to get data into point_location
qry = glue::glue("INSERT INTO beach_boundary_history ",
                 "SELECT CAST(beach_boundary_history_id AS UUID), CAST(beach_id AS UUID), ",
                 "CAST(survey_type_id AS UUID), beach_number, beach_name, CAST(active_indicator AS boolean), ",
                 "CAST(active_datetime AS timestamptz), CAST(inactive_datetime AS timestamptz), ",
                 "inactive_reason, gid, geometry AS geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM beach_history_temp")

# Insert select to local shellfish
db_con = pg_con_local(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Insert select to prod shellfish
db_con = pg_con_prod(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_local(dbname = "shellfish")
DBI::dbExecute(db_con, "DROP TABLE beach_history_temp")
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "shellfish")
DBI::dbExecute(db_con, "DROP TABLE beach_history_temp")
DBI::dbDisconnect(db_con)

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

