#=================================================================
# Load 2020 seasons data to new DB
#
# NOTES:
#  !. Using xlsx from Kat this year...much nicer
#  2. Keeping all beaches with actual season closures
#     in seasons file, even if they were not surveyed.
#  3. Eagle Creek and Pt Whitney Tidelands were both missing a trailing
#     character for beach_id. Corrected now in xlsx.
#  3. All uploaded on 2020-11-
#
# AS 2020-11-05
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
library(stringi)
library(openxlsx)
library(lubridate)
library(uuid)

# Keep connections pane from opening
options("connectionObserver" = NULL)

# Set year
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

#======================================================================================
# Get beach data
#======================================================================================

# Import the beach data from shellfish DB
qry = glue::glue("select beach_id, beach_number as bidn, beach_name, active_datetime, inactive_datetime ",
                 "from beach_boundary_history")

# Get existing beach data from DB...No BIDNs
db_con = pg_con_local(dbname = "shellfish")
beach = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Get start and end years for beaches
beach = beach %>%
  mutate(start_yr = as.integer(substr(active_datetime, 1, 4))) %>%
  mutate(end_yr = as.integer(substr(inactive_datetime, 1, 4)))

# Check
sort(unique(beach$start_yr))
sort(unique(beach$end_yr))

#======================================================================================
# Get seasons data
#======================================================================================

# Get the seasons data
seayr = read.xlsx(glue("Seasons/data/beach_seasons_all_{current_year}.xlsx"), detectDates = TRUE)

# Format dates
seayr = seayr %>%
  mutate(season_start = format(season_start)) %>%
  mutate(season_end = format(season_end)) %>%
  mutate(bidn = as.integer(bidn)) %>%
  mutate(season_description = as.character(season_description)) %>%
  mutate(comment_text = as.character(comment_text))

# Verify that year values are all correct
unique(substr(seayr$season_start, 1, 4))
unique(substr(seayr$season_end, 1, 4))

# Get the beach data for current year
bchyr = beach %>%
  filter(start_yr <= current_year & end_yr >= current_year) %>%
  distinct()

# Get the beach data
bch_all = beach %>%
  select(beach_id, bidn, beach_name) %>%
  distinct()

# Pull out only the first occurrance of beach_id
bch_all = bch_all %>%
  group_by(beach_id) %>%
  mutate(nseq = row_number(beach_id)) %>%
  ungroup() %>%
  arrange(bidn, nseq) %>%
  filter(nseq == 1L) %>%
  select(bidn, beach_id_all = beach_id, beach_name_all = beach_name)

# Warn for duplicated bidns in bchyr
if (any(duplicated(bchyr$bidn))) {
  cat("\nWarning: Duplicated BIDN. Investigate!\n\n")
}

# Warn for duplicated bidns in bch_all
if (any(duplicated(bch_all$bidn))) {
  cat("\nWarning: Duplicated BIDN. Investigate!\n\n")
}

# Check how Dose appears in the beach file
chk_dose_bch = bchyr %>%
  filter(bidn %in% c(270200L, 270201L))

# Check how Dose appears in the seasons file
chk_dose_sea = seayr %>%
  filter(bidn %in% c(270200L, 270201L))

# Warn if Dose needs to be updated from 270200 to 270201
if (chk_dose_bch$bidn %in% chk_dose_sea$bidn) {
  cat("\nDosewallips defined the same in seasons and bchyr files. Ok to proceed.\n\n")
} else {
  cat("\nDosewallips will be re-defined below.\n\n")
  # Redefine dose
  dose_bidn = unique(chk_dose_bch$bidn)
  seayr = seayr %>%
    mutate(bidn = if_else(bidn %in% c(270200L, 270201L), dose_bidn, bidn))
}

# Check how Ala Spit appears in the seasons file...it should be 240265
chk_ala = seayr %>%
  filter(bidn %in% c(240260, 240265))

# Update ala spit bidn
seayr = seayr %>%
  mutate(bidn = if_else(bidn == 240260L, 240265L, bidn))

#=========================================================================================
# Verify beach_ids are correct !!!
#=========================================================================================

# Verify beach_ids are correct in seayr
beach_chk = seayr %>%
  select(sea_beach_id = beach_id, bidn, sea_beach_name = beach_name,
         species_group_code, season_start, season_end) %>%
  left_join(bchyr, by = "bidn") %>%
  left_join(bch_all, by = "bidn") %>%
  mutate(not_in_year_polys = if_else(!sea_beach_id == beach_id, "yes", "no")) %>%
  mutate(not_in_beach_all = if_else(!sea_beach_id == beach_id_all, "yes", "no"))

# Verify data for beaches that are in seasons file but not in current year beach polygons
# These were not surveyed
chk_year_poly = beach_chk %>%
  filter(is.na(not_in_year_polys) | not_in_year_polys == "yes")

# Warn if any cases. For 2020 These were Ok. Just Dungeness, Mystery Bay, and Spencer Spit.
if ( nrow(chk_year_poly) > 0 ) {
  cat("\nWARNING: Some beaches in seasons file are not in beach polygons for current year. Verify these are correct!\n\n")
} else {
  cat("\nAll beaches in seasons file are present in current year beach polygons. Ok to proceed.\n\n")
}

# Verify data for beaches that are in seasons file but not in all years beach polygons
# Might indicate error in beach_ids or bidns
chk_all_poly = beach_chk %>%
  filter(is.na(not_in_beach_all) | not_in_beach_all == "yes")

# Warn if any cases
if ( nrow(chk_all_poly) > 0 ) {
  cat("\nWARNING: Some beach_ids in seasons file appear incorrect. Inspect for data entry errors! Do not pass go!!\n\n")
} else {
  cat("\nBeach IDs in seasons file match IDs in full set of beach polygons. Ok to proceed.\n\n")
}

#==========================================================================================
# Prep for upload
#==========================================================================================

# Check values needing to be converted
unique(seayr$season_status_code)
unique(seayr$species_group_code)

# Add final columns
season_table = seayr %>%
  mutate(beach_season_id = get_uuid(nrow(seayr))) %>%
  mutate(season_status_id = if_else(season_status_code == "OR",
                                    "81e9cbb4-4bbd-46fd-aab8-bc824d523d22",
                                    "61e84153-946f-4e12-ade0-def83b57c0d9")) %>%
  mutate(species_group_id = if_else(species_group_code == "Clam",
                                    "65fcd0ba-d701-4ee1-9e1e-960bad82ece2",
                                    "379db4d3-ec40-4ebf-b588-6f87d68ec303")) %>%
  mutate(season_start_datetime = with_tz(as.POSIXct(season_start, tz = "America/Los_Angeles"), tzone = "UTC")) %>%
  mutate(season_end_datetime = with_tz(as.POSIXct(season_end, tz = "America/Los_Angeles"), tzone = "UTC")) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = Sys.getenv("USERNAME")) %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(beach_season_id, beach_id, season_status_id, species_group_id,
         season_start_datetime, season_end_datetime, season_description,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

# Check all required fields are present....All must be FALSE
any(is.na(season_table$beach_season_id))
any(is.na(season_table$beach_id))
any(is.na(season_table$season_status_id))
any(is.na(season_table$species_group_id))
any(is.na(season_table$season_start_datetime))
any(is.na(season_table$season_end_datetime))
any(is.na(season_table$created_datetime))
any(is.na(season_table$created_by))

# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "beach_season", season_table, row.names = FALSE, append = TRUE)
# dbDisconnect(db_con)
#
# # Write to shellfish_archive
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "beach_season", season_table, row.names = FALSE, append = TRUE)
# dbDisconnect(db_con)






