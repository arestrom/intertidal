#=================================================================
# Load 2019 seasons data to new DB
# 
# NOTES: 
#  !. Added code to adjust Dose to 270201 when only 270201 and not
#     270200 was in bchyr file.
#  2. For possible future use, decided to keep all beaches with
#     actual season closures in seasons file, even if they were
#     not surveyed. 
#  3. All uploaded on 2019-11-04
#
# AS 2019-11-04
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

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
seayr = read.xlsx(glue::glue("season_{year}_std.xlsx"), detectDates = TRUE)

# Check comments
unique(seayr$`Changes?`)

# Rename columns
seayr = seayr %>%
  select(bidn = BIDN, beach_season_name = BeachName, begin = Begin,
         end = End, clam_sea = ClamSea, oys_sea = OysterSea)

# Format dates
seayr = seayr %>% 
  mutate(begin = format(begin)) %>% 
  mutate(end = format(end)) %>% 
  mutate(bidn = as.integer(bidn))

# Verify that year values are all correct
unique(substr(seayr$begin, 1, 4))
unique(substr(seayr$end, 1, 4))

# Get the beach data for current year
bchyr = beach %>% 
  filter(start_yr <= year & end_yr >= year) %>% 
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

# Update 
seayr = seayr %>% 
  mutate(bidn = if_else(bidn == 240260L, 240265L, bidn))

# Add beach_id from current year beach table
sea = seayr %>% 
  left_join(bchyr, by = "bidn") %>% 
  left_join(bch_all, by = "bidn")

# Trim to needed variables
sea = sea %>% 
  mutate(beach_id = if_else(is.na(beach_id) & !is.na(beach_id_all), beach_id_all, beach_id)) %>% 
  mutate(beach_name = if_else(is.na(beach_name) & !is.na(beach_name_all), beach_name_all, beach_name)) %>% 
  select(beach_id, bidn, beach_season_name, beach_name, beach_name_all, begin, end, clam_sea, oys_sea,
         start_yr, end_yr)

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
  mutate(season_status_id = if_else(sea_status == "O", "81e9cbb4-4bbd-46fd-aab8-bc824d523d22", 
                                    "61e84153-946f-4e12-ade0-def83b57c0d9")) %>% 
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

# Write to shellfish
db_con = pg_con_local(dbname = "shellfish")
DBI::dbWriteTable(db_con, "beach_season", season_table, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)

# Write to shellfish_archive
db_con = pg_con_local(dbname = "shellfish_archive")
DBI::dbWriteTable(db_con, "beach_season", season_table, row.names = FALSE, append = TRUE)
dbDisconnect(db_con)






