#=================================================================
# Kat loaded Flight data to prod. Need to copy to my local.
# Need to make sure that uuid keys match
#
# NOTES:
#  1.
#
# All loaded to local on 2020-11-16
#
# AS 2020-11-16
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
# Identify current year flight data that has been loaded to production
#==============================================================================

# Pull out and verify IDs loaded by Kat
qry = glue::glue("select s.survey_id, se.survey_event_id, ",
                 "pt.point_location_id ",
                 "from survey as s ",
                 "left join survey_event as se on s.survey_id = se.survey_id ",
                 "left join point_location as pt on se.event_location_id = pt.point_location_id ",
                 "where date_part('year', survey_datetime) = 2020")

db_con = pg_con_prod(dbname = "shellfish")
prod_ids = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify no flight data for the year was loaded to my local
qry = glue::glue("select s.survey_id, se.survey_event_id, ",
                 "pt.point_location_id ",
                 "from survey as s ",
                 "left join survey_event as se on s.survey_id = se.survey_id ",
                 "left join point_location as pt on se.event_location_id = pt.point_location_id ",
                 "where date_part('year', survey_datetime) = 2020")

db_con = pg_con_local(dbname = "shellfish")
local_ids = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Verify counts match: Result...None of the flight data have been uploaded to local
if ( nrow(prod_ids) == max(diff_counts$row_diff) ) {
  cat("\nCorrect number of rows pulled. Ok to proceed.\n\n")
} else {
  cat("\nWARNING: Row counts not as expected. Do not pass go!\n\n")
}

#==============================================================================
# Write the point_location data
#==============================================================================

# Get the missing point_location_data from prod
pt_ids = unique(prod_ids$point_location_id)
length(pt_ids)
pt_ids = paste0(paste0("'", pt_ids, "'"), collapse = ", ")

# Define query
qry = glue("select * from point_location ",
           "where point_location_id in ({pt_ids})")

db_con = pg_con_prod(dbname = "shellfish")
pt_loc = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check timezone...None set
tz(pt_loc$created_datetime)[1]

# Set and convert timezone
pt_loc = pt_loc %>%
  mutate(created_datetime = with_tz(as.POSIXct(created_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(modified_datetime = with_tz(as.POSIXct(modified_datetime, tz = "America/Los_Angeles"), "UTC"))

# Check timezone again
tz(pt_loc$created_datetime)[1]

# Write to local
db_con = pg_con_local(dbname = "shellfish")
DBI::dbWriteTable(db_con, "point_location", pt_loc, row.names = FALSE, append = TRUE)
DBI::dbDisconnect(db_con)

#==============================================================================
# Write the survey data
#==============================================================================

# Get the missing point_location_data from prod
s_ids = unique(prod_ids$survey_id)
length(s_ids)
s_ids = paste0(paste0("'", s_ids, "'"), collapse = ", ")

# Define query
qry = glue("select * from survey ",
           "where survey_id in ({s_ids})")

db_con = pg_con_prod(dbname = "shellfish")
survey = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check timezone...None set
tz(survey$created_datetime)[1]

# Set and convert timezone
survey = survey %>%
  mutate(survey_datetime = with_tz(as.POSIXct(survey_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(start_datetime = with_tz(as.POSIXct(start_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(end_datetime = with_tz(as.POSIXct(end_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(created_datetime = with_tz(as.POSIXct(created_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(modified_datetime = with_tz(as.POSIXct(modified_datetime, tz = "America/Los_Angeles"), "UTC"))

# Check timezone again
tz(survey$created_datetime)[1]

# Write to local
db_con = pg_con_local(dbname = "shellfish")
DBI::dbWriteTable(db_con, "survey", survey, row.names = FALSE, append = TRUE)
DBI::dbDisconnect(db_con)

#==============================================================================
# Write the survey_event data
#==============================================================================

# Get the missing point_location_data from prod
se_ids = unique(prod_ids$survey_event_id)
length(se_ids)
se_ids = paste0(paste0("'", se_ids, "'"), collapse = ", ")

# Define query
qry = glue("select * from survey_event ",
           "where survey_event_id in ({se_ids})")

db_con = pg_con_prod(dbname = "shellfish")
survey_event = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check timezone...None set
tz(survey_event$created_datetime)[1]

# Set and convert timezone
survey_event = survey_event %>%
  mutate(created_datetime = with_tz(as.POSIXct(created_datetime, tz = "America/Los_Angeles"), "UTC")) %>%
  mutate(modified_datetime = with_tz(as.POSIXct(modified_datetime, tz = "America/Los_Angeles"), "UTC"))

# Check timezone again
tz(survey_event$created_datetime)[1]

# Write to local
db_con = pg_con_local(dbname = "shellfish")
DBI::dbWriteTable(db_con, "survey_event", survey_event, row.names = FALSE, append = TRUE)
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

#==============================================================================
# Verify time values match....issue in first run of code
#==============================================================================

# Pull out and verify IDs loaded by Kat
qry = glue::glue("select s.survey_id, se.survey_event_id, ",
                 "pt.point_location_id ",
                 "from survey as s ",
                 "left join survey_event as se on s.survey_id = se.survey_id ",
                 "left join point_location as pt on se.event_location_id = pt.point_location_id ",
                 "where date_part('year', survey_datetime) = 2020")

db_con = pg_con_prod(dbname = "shellfish")
prod_ids = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

#================================================
# Check survey: Result...times match
#================================================

# Get the IDs
s_ids = unique(prod_ids$survey_id)
length(s_ids)
s_ids = paste0(paste0("'", s_ids, "'"), collapse = ", ")

# Define query
qry = glue("select survey_id, survey_datetime as survey_time_prod, ",
           "start_datetime as start_time_prod, end_datetime as end_time_prod ",
           "from survey ",
           "where survey_id in ({s_ids})")

db_con = pg_con_prod(dbname = "shellfish")
chk_survey_prod = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Define query
qry = glue("select survey_id, survey_datetime as survey_time_local, ",
           "start_datetime as start_time_local, end_datetime as end_time_local ",
           "from survey ",
           "where survey_id in ({s_ids})")

db_con = pg_con_local(dbname = "shellfish")
chk_survey_local = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Join...Dates are correct
chk_survey = chk_survey_prod %>%
  left_join(chk_survey_local, by = "survey_id") %>%
  filter(!survey_time_prod == survey_time_local)

#================================================
# Check survey_event: Result...times match
#================================================

# Get the IDs
se_ids = unique(prod_ids$survey_event_id)
length(se_ids)
se_ids = paste0(paste0("'", se_ids, "'"), collapse = ", ")

# Define query
qry = glue("select survey_event_id, event_datetime as event_time_prod, ",
           "created_datetime as create_time_prod ",
           "from survey_event ",
           "where survey_event_id in ({se_ids})")

db_con = pg_con_prod(dbname = "shellfish")
chk_survey_event_prod = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Define query
qry = glue("select survey_event_id, event_datetime as event_time_local, ",
           "created_datetime as create_time_local ",
           "from survey_event ",
           "where survey_event_id in ({se_ids})")

db_con = pg_con_local(dbname = "shellfish")
chk_survey_event_local = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Join...Dates are correct
chk_survey_event = chk_survey_event_prod %>%
  left_join(chk_survey_event_local, by = "survey_event_id") %>%
  filter(!event_time_prod == event_time_local)





