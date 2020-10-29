#=================================================================
# Delete all newly entered flight data for the current year from DB
#
# NOTES:
#  1. Dumped from local, and prod
#
# AS 2020-10-29
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Libraries
library(dplyr)
library(DBI)
library(RPostgres)
library(glue)

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

#==============================================================================
# Check flight data currently in DB
#==============================================================================

# Import the beach data from shellfish DB
qry = glue("select s.survey_id, st.survey_type_description, se.survey_event_id, ",
           "se.event_location_id as point_location_id, date_part('year', s.survey_datetime) as year, ",
           "s.created_datetime ",
           "from survey as s inner join survey_type_lut as st ",
           "on s.survey_type_id = st.survey_type_id ",
           "left join survey_event as se on s.survey_id = se.survey_id ",
           "where survey_type_code in ('Clam aerial', 'Clam ground') ",
           "and date_part('year', survey_datetime) = {current_year}")

# Read
db_con = pg_con_local(dbname = "shellfish")
flt_dat = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Check survey_type
unique(flt_dat$survey_type_description)

# Check survey table created_date
unique(flt_dat$created_datetime)

# Check years of flight surveys
unique(flt_dat$year)

# Create vector of survey_ids
s_ids = paste0(paste0("'", unique(flt_dat$survey_id), "'"), collapse = ", ")

# Create vector of survey_event_ids
se_ids = unique(flt_dat$survey_event_id)
se_ids = se_ids[!is.na(se_ids)]
se_ids = paste0(paste0("'", se_ids, "'"), collapse = ", ")

# Create vector of point_location_ids
pt_ids = unique(flt_dat$point_location_id)
pt_ids = pt_ids[!is.na(pt_ids)]
pt_ids = paste0(paste0("'", pt_ids, "'"), collapse = ", ")

#======================================================================
# Delete previously entered flight data so it can be re-entered
# Use conditions for anything larger....
#======================================================================

# # Dump from local shellfish ===============================
#
# # Dump survey_event data
# db_con = pg_con_local(dbname = "shellfish")
# dbExecute(db_con,
#           glue::glue("DELETE FROM survey_event ",
#                      "WHERE survey_event_id IN ({se_ids})"))
# dbDisconnect(db_con)
#
# # Dump point_location data
# db_con = pg_con_local(dbname = "shellfish")
# dbExecute(db_con,
#           glue::glue("DELETE FROM point_location ",
#                      "WHERE point_location_id IN ({pt_ids})"))
# dbDisconnect(db_con)
#
# # Dump survey data
# db_con = pg_con_local(dbname = "shellfish")
# dbExecute(db_con,
#           glue::glue("DELETE FROM survey ",
#                      "WHERE survey_id IN ({s_ids})"))
# dbDisconnect(db_con)
#
# # Dump from prod shellfish ===============================
#
# # Dump survey_event data
# db_con = pg_con_prod(dbname = "shellfish")
# dbExecute(db_con,
#           glue::glue("DELETE FROM survey_event ",
#                      "WHERE survey_event_id IN ({se_ids})"))
# dbDisconnect(db_con)
#
# # Dump point_location data
# db_con = pg_con_prod(dbname = "shellfish")
# dbExecute(db_con,
#           glue::glue("DELETE FROM point_location ",
#                      "WHERE point_location_id IN ({pt_ids})"))
# dbDisconnect(db_con)
#
# # Dump survey data
# db_con = pg_con_prod(dbname = "shellfish")
# dbExecute(db_con,
#           glue::glue("DELETE FROM survey ",
#                      "WHERE survey_id IN ({s_ids})"))
# dbDisconnect(db_con)

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
