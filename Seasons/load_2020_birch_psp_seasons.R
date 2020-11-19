#=================================================================
# Load 2019 Birch Bay PSP seasons data to new DB
# 
# NOTES: 
#  1. All uploaded on 2019-11-04
#
# AS 2019-12-03
#=================================================================

# Clear workspace
rm(list=ls(all=TRUE))

# Libraries
library(remisc)
library(dplyr)
library(DBI)
library(RODBC)
library(glue)
library(sf)
library(stringi)
library(openxlsx)
library(lubridate)

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
    RPostgreSQL::PostgreSQL(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

# Function to connect to postgres
pg_con_test = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgreSQL::PostgreSQL(),
    host = pg_host("pg_host_test"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_test"),
    port = port)
  con
}

# Function to connect to postgres
pg_con_prod = function(dbname, port = '5432') {
  con <- dbConnect(
    RPostgreSQL::PostgreSQL(),
    host = pg_host("pg_host_prod"),
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_prod"),
    port = port)
  con
}

# Function to generate dataframe of tables and row counts in database
db_row_counts = function(hostname = "local", dbname) {
  if (hostname == "local") {
    db_con = pg_con_local(dbname = dbname)
  } else if (hostname == "test") {
    db_con = pg_con_test(dbname = dbname)
  } else if (hostname == "prod") {
    db_con = pg_con_prod(dbname = dbname)
  }
  db_tables = dbListTables(db_con)
  tabx = integer(length(db_tables))
  get_count = function(i) {
    tabxi = dbGetQuery(db_con, paste0("select count(*) from ", db_tables[i]))
    as.integer(tabxi$count)
  }
  rc = lapply(seq_along(tabx), get_count)
  dbDisconnect(db_con)
  rcx = as.integer(unlist(rc))
  dtx = data_frame(table = db_tables, row_count = rcx)
  dtx = dtx %>% 
    arrange(table)
  dtx
}

#======================================================================================
# Get beach data
#======================================================================================

# Import the beach data from shellfish DB
qry = glue::glue("select beach_id, beach_number as bidn, beach_name, active_datetime, inactive_datetime ",
                 "from beach_boundary_history")

# Get existing beach data from DB...No BIDNs
db_con = dbConnect(odbc::odbc(), timezone = "UTC", dsn = "local_shellfish")
beach = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Get start and end years for beaches
beach = beach %>% 
  mutate(start_yr = as.integer(substr(active_datetime, 1, 4))) %>% 
  mutate(end_yr = as.integer(substr(inactive_datetime, 1, 4))) %>% 
  mutate(end_yr = if_else(substr(inactive_datetime, 6, 7) == "01", 
                          end_yr - 1L, end_yr))

#======================================================================================
# Get seasons data
#======================================================================================

# Set year
year = 2019L

# Get the seasons data
psp_sea = read.xlsx(glue::glue("birch_psp_season.xlsx"), detectDates = TRUE)

# Rename columns
psp_sea = psp_sea %>%
  mutate(clam_sea = substr(crlsea, 1, 1)) %>% 
  mutate(oys_sea = substr(crlsea, 2, 2)) %>% 
  select(bidn, beach_id, beach_name, year,
         begin, end, clam_sea, oys_sea)

# Format dates
psp_sea = psp_sea %>%
  mutate(begin = format(begin)) %>% 
  mutate(end = format(end)) %>% 
  mutate(bidn = as.integer(bidn))

# Verify that year values are all correct
unique(substr(psp_sea$begin, 1, 4))
unique(substr(psp_sea$end, 1, 4))

# Trim to needed variables
sea = psp_sea %>%
  mutate(start_yr = year) %>% 
  select(beach_id, bidn, beach_name, begin, end, clam_sea, oys_sea,
         start_yr, end_yr = year)

# Check beaches being dropped due to no beach_id
chk_sea_drop = sea %>% 
  filter(is.na(beach_id))

# Drop beaches without beach_id
sea = sea %>%
  filter(!is.na(beach_id))

# Stop to inspect beach names
cat("\nStop for a second to inspect beach names and beaches not surveyed!\n\n")

# Pull out sections for seasons table
sea_clam = sea %>%
  mutate(sea_status = clam_sea) %>% 
  mutate(sp_group = "clam") %>% 
  mutate(sea_int = recode(sea_status, 
                          "C" = 1L,
                          "O" = 2L)) %>% 
  group_by(bidn) %>% 
  mutate(sea_mn = mean(sea_int, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(beach_id, bidn, beach_name, sea_status, sp_group, begin, end, sea_mn)

# Pull out sections for seasons table
sea_oys = sea %>% 
  mutate(sea_status = oys_sea) %>% 
  mutate(sp_group = "oyster") %>% 
  mutate(sea_int = recode(sea_status, 
                          "C" = 1L,
                          "O" = 2L)) %>% 
  group_by(bidn) %>% 
  mutate(sea_mn = mean(sea_int, na.rm = TRUE)) %>% 
  ungroup() %>% 
  select(beach_id, bidn, beach_name, sea_status, sp_group, begin, end, sea_mn)

# Combine
sea_tab = rbind(sea_clam, sea_oys)

# Arrange
sea_tab = sea_tab %>% 
  arrange(bidn, begin, sp_group) %>% 
  group_by(bidn) %>% 
  mutate(sea_sum = sum(sea_mn)) %>% 
  ungroup()

# Drop sea_sum == 4
sea_tab = sea_tab %>% 
  filter(!sea_sum == 4)

# Add final columns
season_table = sea_tab %>% 
  mutate(beach_season_id = remisc::get_uuid(nrow(sea_tab))) %>% 
  mutate(season_status_id = if_else(sea_status == "O", "9b44fcf1-0d9d-4a14-a9cf-a8a526c0589e", 
                                    "d012f782-1e89-497b-9f35-d3db28a6d75a")) %>% 
  mutate(species_group_id = if_else(sp_group == "clam", "65fcd0ba-d701-4ee1-9e1e-960bad82ece2",
                                    "379db4d3-ec40-4ebf-b588-6f87d68ec303")) %>%
  mutate(begin = with_tz(as.POSIXct(begin, tz = "America/Los_Angeles"), tzone = "UTC")) %>% 
  mutate(end = with_tz(as.POSIXct(end, tz = "America/Los_Angeles"), tzone = "UTC")) %>% 
  mutate(season_description = NA_character_) %>% 
  mutate(comment_text = NA_character_) %>% 
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(beach_season_id, beach_id, season_status_id, species_group_id,
         season_start_datetime = begin, season_end_datetime = end, season_description,
         comment_text, created_datetime, created_by, modified_datetime, modified_by)

# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "beach_season", season_table, row.names = FALSE, append = TRUE)
# dbDisconnect(db_con)






