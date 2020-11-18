#=================================================================
# Replace the local version of split potlatch BIDN with corrected
# version from the prod database
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
# Delete previously uploaded Potlatch beach table data...2020 data
#==============================================================================

# Inspect Potlatch beach_boundary_history polygons for 2020 that were previously loaded
qry = glue::glue("select * from beach_boundary_history ",
                 "where beach_number in (270440, 270442) ",
                 "and active_datetime = '2020-01-01 08:00:00+00'")
db_con = pg_con_local(dbname = "shellfish")
chk_delete = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Get vector off beach_boundary_ids that need to be deleted
bb_ids = unique(chk_delete$beach_boundary_history_id)
bb_ids = paste0(paste0("'", bb_ids, "'"), collapse = ", ")

# Get rid of all beach_boundary_history polygons for Potlatch in 2020 that were previously loaded
qry = glue::glue("delete from beach_boundary_history ",
                 "where beach_boundary_history_id in ({bb_ids})")

db_con = pg_con_local(dbname = "shellfish")
DBI::dbExecute(db_con, qry)
dbDisconnect(db_con)

#==================================================================================
# Get 2020 Potlatch polygon from prod that needs to be added to local
#==================================================================================

# Pull data from beach_boundary_history 2019....
qry = glue("select * ",
           "from beach_boundary_history ",
           "where beach_number in (270440) ",
           "and active_datetime = '2020-01-01 08:00:00+00'")

# Run the query
db_con = pg_con_prod(dbname = "shellfish")
potlatch = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly set timezone, first to local, then to UTC for writing....Should see the 08:00:00 time structure appear
potlatch = potlatch %>%
  mutate(active_datetime = with_tz(as.POSIXct(active_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(inactive_datetime = with_tz(as.POSIXct(inactive_datetime, tz = "America/Los_Angeles"), "UTC"))

# Verify gid in potlatch does not already exist in local

# First get gid for potlatch in prod
potlatch_gid = potlatch$gid

# Define query for local
qry = glue("select * from beach_boundary_history ",
           "where gid = {potlatch_gid}")

# See if anything gets pulled from local...It should be empty
db_con = pg_con_local(dbname = "shellfish")
chk_gid = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Warn if different gids
if ( nrow(chk_gid) > 0L ) {
  cat("\nWARNING: GID already exists. Investigate. Do not pass go!\n\n")
} else {
  cat("GID is available. Ok to proceed.\n\n")
}

#=========================================================================================
# Write to local. Can use normal writeTable function since geometry is already in binary
#=========================================================================================

# Write to local
db_con = pg_con_local(dbname = "shellfish")
DBI::dbWriteTable(db_con, "beach_boundary_history", potlatch, row.names = FALSE, append = TRUE)
DBI::dbDisconnect(db_con)



