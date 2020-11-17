#=================================================================
# Kat loaded Seasons data to prod. Need to copy to my local.
# Need to make sure that uuid keys match
#
# NOTES:
#  1.
#
# All loaded to local on 2020-11-17
#
# AS 2020-11-17
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

#==============================================================================
# Identify current year Seasons data that has been loaded to production
#==============================================================================

# Pull out and verify IDs loaded by Kat
qry = glue::glue("select beach_allowance_id ",
                 "from beach_allowance ",
                 "where allowance_year = 2020")

db_con = pg_con_prod(dbname = "shellfish")
prod_ids = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

db_con = pg_con_local(dbname = "shellfish")
local_ids = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify counts match: Result...None of the flight data have been uploaded to local
prod_season_count = diff_counts %>%
  filter(table == "beach_allowance") %>%
  pull(row_diff)

if ( nrow(prod_ids) == prod_season_count ) {
  cat("\nCorrect number of rows pulled. Ok to proceed.\n\n")
} else {
  cat("\nWARNING: Row counts not as expected. Do not pass go!\n\n")
}

#==============================================================================
# Write the point_location data
#==============================================================================

# Get the missing point_location_data from prod
ba_ids = unique(prod_ids$beach_allowance_id)
length(ba_ids)
ba_ids = paste0(paste0("'", ba_ids, "'"), collapse = ", ")

# Define query
qry = glue("select * from beach_allowance ",
           "where beach_allowance_id in ({ba_ids})")

db_con = pg_con_prod(dbname = "shellfish")
beach_allowance = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check timezone...None set
tz(beach_allowance$created_datetime)[1]

# Set and convert timezone
beach_allowance = beach_allowance %>%
  mutate(created_datetime = with_tz(as.POSIXct(created_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(modified_datetime = with_tz(as.POSIXct(modified_datetime, tz = "America/Los_Angeles"), "UTC"))

# Check timezone again
tz(beach_allowance$created_datetime)[1]

# Write to local
db_con = pg_con_local(dbname = "shellfish")
DBI::dbWriteTable(db_con, "beach_allowance", beach_allowance, row.names = FALSE, append = TRUE)
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

