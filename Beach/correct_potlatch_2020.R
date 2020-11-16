#=================================================================
# Replace split Potlatch polygons in 2020 with 2019 version
# 
# NOTES: 
#  1. Potlatch Beaches for 2020 were updated to single polygon
#     on 2020-11-16
# 
# ToDo:
#  1. 
#
# AS 2020-11-16
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Libraries
library(remisc)
library(dplyr)
library(DBI)
library(RPostgres)
library(glue)
library(sf)
library(lubridate)
library(stringi)
library(tibble)
library(uuid)

# Set options
options(digits=14)

# Keep connections pane from opening
options("connectionObserver" = NULL)

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

# #============================================================================================
# # Verify the same number of rows exist in both local and production DBs
# #============================================================================================
# 
# # Get table names and row counts
# local_row_counts = db_table_counts(db_server = "local")
# prod_row_counts = db_table_counts(db_server = "prod")
# 
# # Combine to a dataframe
# compare_counts = local_row_counts %>%
#   left_join(prod_row_counts, by = "table") %>%
#   # Ignore tables that exist in local but not prod
#   filter(!table %in% c("geometry_columns", "geometry_columns", "spatial_ref_sys")) %>%
#   filter(!substr(table, 1, 10) == "beach_info") %>%
#   filter(!substr(table, 1, 12) == "flight_count") %>%
#   # Pull out and rename
#   select(table, local = row_count.x, prod = row_count.y) %>%
#   mutate(row_diff = abs(local - prod))
# 
# # Inspect any differences
# diff_counts = compare_counts %>%
#   filter(!row_diff == 0L)
# 
# # Output message
# if ( nrow(diff_counts) > 0 ) {
#   cat("\nWARNING: Some row counts differ. Inspect 'diff_counts'.\n\n")
# } else {
#   cat("\nRow counts are the same. Ok to proceed.\n\n")
# }

#==============================================================================
# Delete previously uploaded Potlatch beach table data...2020 data
#==============================================================================

# Inspect Potlatch beach_boundary_history polygons for 2020 that were previously loaded
qry = glue::glue("select * from beach_boundary_history ",
                 "where beach_number in (270440, 270442) ",
                 "and active_datetime = '2020-01-01 08:00:00+00'")
db_con = pg_con_prod(dbname = "shellfish")
chk_delete = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Get vector off beach_boundary_ids that need to be deleted
bb_ids = unique(chk_delete$beach_boundary_history_id)
bb_ids = paste0(paste0("'", bb_ids, "'"), collapse = ", ")

# Get rid of all beach_boundary_history polygons for Potlatch in 2020 that were previously loaded
qry = glue::glue("delete from beach_boundary_history ",
                 "where beach_boundary_history_id in ({bb_ids})")

db_con = pg_con_prod(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

db_con = pg_con_local(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

#==================================================================================
# Get 2019 Potlatch polygon that needs to be added for 2020 
#================================================================================== 

# Pull data from beach_boundary_history 2019....
qry = glue("select beach_id, beach_name, beach_number as bidn, geom as geometry ",
           "from beach_boundary_history ",
           "where beach_number in (270440) ",
           "and active_datetime = '2019-01-01 08:00:00+00'")

# Run the query
db_con = pg_con_prod(dbname = "shellfish")
potlatch = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Add active_datetime stuff
potlatch = potlatch %>% 
  mutate(active_datetime = with_tz(as.POSIXct("2020-01-01", tz = "America/Los_Angeles"), "UTC")) %>% 
  mutate(inactive_datetime = with_tz(as.POSIXct("2020-12-31", tz = "America/Los_Angeles"), "UTC"))
  
# Verify crs is identical
st_crs(potlatch)$epsg

# Get most recent gid
qry = glue("select max(gid) from beach_boundary_history")

# Get values from shellfish_prod
db_con = pg_con_prod(dbname = "shellfish")
max_gid_prod = dbGetQuery(db_con, qry) %>% 
  pull(max)
dbDisconnect(db_con)

# Get values from shellfish_local
db_con = pg_con_prod(dbname = "shellfish")
max_gid_local = dbGetQuery(db_con, qry) %>% 
  pull(max)
dbDisconnect(db_con)

# Warn if different gids
if ( !max_gid_local == max_gid_prod ) {
  cat("\nWARNING: GIDs differ. Investigate. Do not pass go!\n\n") 
} else {
  cat("GIDs are identical. Ok to proceed.\n\n")
}

# Add one
new_gid = max_gid_prod + 1L

# Fill in names where missing
new_flt_beach = potlatch %>% 
  mutate(gid = new_gid) %>% 
  mutate(beach_boundary_history_id = get_uuid(1L)) %>%
  mutate(survey_type_id = "7153c1cb-79cf-41d6-9fc1-966470c1460b") %>% 
  mutate(active_indicator = 0L) %>% 
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

# Check names of beaches
sort(unique(new_flt_beach$beach_name))
sort(unique(new_flt_beach$beach_number))

# Verify all variables are present
any(is.na(new_flt_beach$beach_boundary_history_id))
any(is.na(new_flt_beach$beach_id)) 
any(is.na(new_flt_beach$survey_type_id))
any(is.na(new_flt_beach$beach_number))
any(is.na(new_flt_beach$beach_name))
any(is.na(new_flt_beach$active_indicator))
any(is.na(new_flt_beach$active_datetime))
any(is.na(new_flt_beach$inactive_datetime))
any(is.na(new_flt_beach$gid)) 
any(is.na(new_flt_beach$geometry))
any(is.na(new_flt_beach$created_datetime))
any(is.na(new_flt_beach$created_by))

#=========================================================================================
# Write temp tables
#=========================================================================================

# Write beach_history_temp to shellfish
db_con = pg_con_local(dbname = "shellfish")
st_write(obj = new_flt_beach, dsn = db_con, layer = "beach_history_temp")
DBI::dbDisconnect(db_con)

# Write beach_history_temp to shellfish
db_con = pg_con_prod(dbname = "shellfish")
st_write(obj = new_flt_beach, dsn = db_con, layer = "beach_history_temp")
DBI::dbDisconnect(db_con)

#=========================================================================================
# Insert select from temp tables
#=========================================================================================

# Use select into query to get data into point_location
qry = glue::glue("INSERT INTO beach_boundary_history ",
                 "SELECT CAST(beach_boundary_history_id AS UUID), CAST(beach_id AS UUID), ",
                 "CAST(survey_type_id AS UUID), beach_number, beach_name, CAST(active_indicator AS boolean), ",
                 "CAST(active_datetime AS timestamptz), CAST(inactive_datetime AS timestamptz), ",
                 "inactive_reason, gid, geometry AS geom, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM beach_history_temp")

# Insert select to shellfish local
db_con = pg_con_local(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

# Insert select to shellfish prod
db_con = pg_con_prod(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
DBI::dbDisconnect(db_con)

#=========================================================================================
# Drop temp tables
#=========================================================================================

# Drop temp
db_con = pg_con_local(dbname = "shellfish")
DBI::dbExecute(db_con, "DROP TABLE beach_history_temp")
DBI::dbDisconnect(db_con)

# Drop temp
db_con = pg_con_prod(dbname = "shellfish")
DBI::dbExecute(db_con, "DROP TABLE beach_history_temp")
DBI::dbDisconnect(db_con)



