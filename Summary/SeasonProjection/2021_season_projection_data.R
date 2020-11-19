#================================================================================================
# Generate data to use in projecting 2021 season
#
#
#  CHANGES FOR 2021 season:
#    1.
#
#  QUESTIONS:
#    1.
#
#  ToDo:
# 	1. Make sure and edit code for current_year minus x....see notes below!!
#
# AS, 2020-11-19
#================================================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Load libraries
library(dplyr)
library(glue)
library(tidyr)
library(DBI)
library(RPostgres)
library(sf)
library(lubridate)
library(chron)
library(tibble)
library(openxlsx)
library(formattable)
library(stringi)

# Keep connections pane from opening
options("connectionObserver" = NULL)
options(dplyr.summarise.inform = FALSE)

# Time the program
strt = Sys.time()

# Set current year variable...the year projections are intended for...in this case 2020
current_year = 2021L

#=======================================================================================
# Define some needed functions. Put in package if useful later ----
#=======================================================================================

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

#===========================================================
# Allowance data ----
#===========================================================

# Read in next years projected allowances
allow = read.xlsx("Summary/SeasonProjection/data/2019_2010_ClamOysterAllowables.xlsx",
                  sheet = "Allowables", detectDates = TRUE)

# Pull out current year data...CHANGE TO MINUS 1 WHEN DATA IS READY !!!!!!!!!!!!!!!!!!!! OR PULL FROM DB !!!!!!!
allow = allow %>%
  filter(allow_year == current_year - 2) %>%
  filter(!(is.na(`allow_lbs(ManNat)`) & is.na(`allow_num(Oyster)`))) %>%
  select(bidn, lallow = `allow_lbs(ManNat)`,
         oallow = `allow_num(Oyster)`)

#===========================================================
# Get cpue and effort data ----
#===========================================================

# Get the CPUE data
qry = glue("select * from mean_cpue_estimate ",
           "where estimation_year >= {current_year - 3}")

db_con = pg_con_local(dbname = "shellfish")
cpue = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Read in mean effort data
qry = glue("select * from mean_effort_estimate ",
           "where estimation_year >= {current_year - 3}")

db_con = pg_con_local(dbname = "shellfish")
effort = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

#===========================================================
# Tide data ----
#===========================================================

# Get tide data
qry = glue("select distinct t.low_tide_datetime as tide_date, ",
           "ts.tide_strata_code as tide_strata ",
           "from tide as t ",
           "left join point_location as pl ",
           "on t.tide_station_location_id = pl.point_location_id ",
           "left join tide_strata_lut as ts ",
           "on t.tide_strata_id = ts.tide_strata_id ",
           "where date_part('year', t.low_tide_datetime) = {current_year} ",
           "and ts.tide_strata_code in ('PLUS', 'HIGH', 'LOW', 'ELOW')")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
tide = dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Explicitly convert timezones
tide = tide %>%
  mutate(tide_date = with_tz(tide_date, tzone = "America/Los_Angeles")) %>%
  mutate(tide_date = format(tide_date)) %>%
  mutate(tide_date = substr(tide_date, 1, 10))

# Check
unique(tide$tide_strata)

#===========================================================
# Cpue AND Opue ----
#===========================================================

# Combine cpue and allowances...CHANGE TO MINUS 1 WHEN DATA IS READY !!!!!!!!!!!!!!!!!!!! OR PULL FROM DB !!!!!!!
clam_cpue = cpue %>%
  filter(species_code %in% c("man", "nat", "but", "oys")) %>%
  filter(estimation_year == current_year - 2) %>%
  select(bidn = beach_number, beach_name, fltsea = flight_season,
         species_code, mean_cpue)

# Filter out cpue values for OO season and pivot to standard format
Cpue = clam_cpue %>%
  filter(fltsea %in% c("OO", "OC")) %>%
  spread(species_code, mean_cpue) %>%
  left_join(allow, by = "bidn") %>%
  select(BIDN = bidn, BeachName = beach_name, LAllow = lallow,
         OAllow = oallow, manila = man, native = nat, butter = but,
         oyster = oys) %>%
  arrange(BIDN)

# Filter out cpue values for OO season and pivot to standard format
Opue = clam_cpue %>%
  filter(fltsea %in% c("CO")) %>%
  spread(species_code, mean_cpue) %>%
  left_join(allow, by = "bidn") %>%
  select(BIDN = bidn, BeachName = beach_name, LAllow = lallow,
         OAllow = oallow, manila = man, native = nat, butter = but,
         oyster = oys) %>%
  arrange(BIDN)

#===========================================================
# Cpro AND Opro ----
#===========================================================

# Create vector of bidns for filter to needed beaches. Camille needed these additional beaches
base_beaches = unique(as.integer(allow$bidn))
base_beaches = c(base_beaches, 250470L, 280680L, 260380L, 280570L,
                 280975L, 281140L, 250512L)
base_beaches = unique(base_beaches)

# Filter effort to beaches in allow
effort = effort %>%
  filter(beach_number %in% base_beaches) %>%
  filter(!flight_season == "CC") %>%
  select(bidn = beach_number, tide_strata, ntds = tide_count,
         mclmuse = mean_effort, est_year = estimation_year,
         fltsea = flight_season)

# Current year - 1 ==========================================

# Create name variable for ntds, current year
ntds_0 = glue("ntds{current_year - 1}")
mc_0 = glue("mc{current_year - 1}")

# Filter to current year OO data for clams
clmyr0 = effort %>%
  filter(fltsea == "OO" & est_year == current_year - 1) %>%
  arrange(bidn, tide_strata) %>%
  select(BIDN = bidn, Strata = tide_strata, ntds, mclmuse)
names(clmyr0) = c("BIDN", "Strata", ntds_0, mc_0)

# Filter to current year CO data for oysters
oysyr0 = effort %>%
  filter(fltsea == "CO" & est_year == current_year - 1) %>%
  arrange(bidn, tide_strata) %>%
  select(BIDN = bidn, Strata = tide_strata, ntds, mclmuse)
names(oysyr0) = c("BIDN", "Strata", ntds_0, mc_0)

# Current year - 2 =======================================

# Create name variable for ntds, current year
ntds_1 = glue("ntds{current_year - 2}")
mc_1 = glue("mc{current_year - 2}")

# Filter to current year OO data for clams
clmyr1 = effort %>%
  filter(fltsea == "OO" & est_year == current_year - 2) %>%
  arrange(bidn, tide_strata) %>%
  select(BIDN = bidn, Strata = tide_strata, ntds, mclmuse)
names(clmyr1) = c("BIDN", "Strata", ntds_1, mc_1)

# Filter to current year CO data for oysters
oysyr1 = effort %>%
  filter(fltsea == "CO" & est_year == current_year - 2) %>%
  arrange(bidn, tide_strata) %>%
  select(BIDN = bidn, Strata = tide_strata, ntds, mclmuse)
names(oysyr1) = c("BIDN", "Strata", ntds_1, mc_1)

# Current year - 3 =======================================

# Create name variable for ntds, current year
ntds_2 = glue("ntds{current_year - 3}")
mc_2 = glue("mc{current_year - 3}")

# Filter to current year OO data for clams
clmyr2 = effort %>%
  filter(fltsea == "OO" & est_year == current_year - 3) %>%
  arrange(bidn, tide_strata) %>%
  select(BIDN = bidn, Strata = tide_strata, ntds, mclmuse)
names(clmyr2) = c("BIDN", "Strata", ntds_2, mc_2)

# Filter to current year CO data for oysters
oysyr2 = effort %>%
  filter(fltsea == "CO" & est_year == current_year - 3) %>%
  arrange(bidn, tide_strata) %>%
  select(BIDN = bidn, Strata = tide_strata, ntds, mclmuse)
names(oysyr2) = c("BIDN", "Strata", ntds_2, mc_2)

# Combine clam files
cp = merge(clmyr0,clmyr1,all=TRUE)
cp = merge(cp,clmyr2, all=TRUE)

# Combine oyster files
op = merge(oysyr0,oysyr1,all=TRUE)
op = merge(op,oysyr2, all=TRUE)

#===========================================================
# Tides ----
#===========================================================

# Add tide data for next year
tides = tide %>%
  mutate(ntds = 1L) %>%
  arrange(as.Date(tide_date))

# Get count of number of tides in each strata
sumtds = tides %>%
  group_by(tide_strata) %>%
  summarize(ntds_next = sum(ntds, na.rm = TRUE)) %>%
  ungroup()
names(sumtds) = c('Strata', glue("ntds{current_year}"))

# Add tides to cp
cp = cp %>%
  left_join(sumtds, by = "Strata") %>%
  arrange(BIDN, Strata)

# Add tides to op
op = op %>%
  left_join(sumtds, by = "Strata") %>%
  arrange(BIDN, Strata)

# Calculate table of tides for each half-month increment for yrp
td = tide %>%
  select(d = tide_date, s = tide_strata) %>%
  mutate(mo = format(as.Date(d), '%b')) %>%
  mutate(day = as.integer(format(as.Date(d), '%d'))) %>%
  mutate(Jan1_15 = if_else(mo == "Jan" & day <= 15, 1, 0)) %>%
  mutate(Jan16_31 = if_else(mo == "Jan" & day > 15, 1, 0)) %>%
  mutate(Feb1_15 = if_else(mo == 'Feb' & day <= 15, 1, 0)) %>%
  mutate(Feb16_28 = if_else(mo == 'Feb' & day > 15, 1, 0)) %>%
  mutate(Mar1_15 = if_else(mo == 'Mar' & day <= 15, 1, 0)) %>%
  mutate(Mar16_31 = if_else(mo == 'Mar' & day > 15, 1, 0)) %>%
  mutate(Apr1_15 = if_else(mo == 'Apr' & day <= 15, 1, 0)) %>%
  mutate(Apr16_30 = if_else(mo == 'Apr' & day > 15, 1, 0)) %>%
  mutate(May1_15 = if_else(mo == 'May' & day <= 15, 1, 0)) %>%
  mutate(May16_31 = if_else(mo == 'May' & day > 15, 1, 0)) %>%
  mutate(Jun1_15 = if_else(mo == 'Jun' & day <= 15, 1, 0)) %>%
  mutate(Jun16_30 = if_else(mo == 'Jun' & day > 15, 1, 0)) %>%
  mutate(Jul1_15 = if_else(mo == 'Jul' & day <= 15, 1, 0)) %>%
  mutate(Jul16_31 = if_else(mo == 'Jul' & day > 15, 1, 0)) %>%
  mutate(Aug1_15 = if_else(mo == 'Aug' & day <= 15, 1, 0)) %>%
  mutate(Aug16_31 = if_else(mo == 'Aug' & day > 15, 1, 0)) %>%
  mutate(Sep1_15 = if_else(mo == 'Sep' & day <= 15, 1, 0)) %>%
  mutate(Sep16_30 = if_else(mo == 'Sep' & day > 15, 1, 0)) %>%
  mutate(Oct1_15 = if_else(mo == 'Oct' & day <= 15, 1, 0)) %>%
  mutate(Oct16_31 = if_else(mo == 'Oct' & day > 15, 1, 0)) %>%
  mutate(Nov1_15 = if_else(mo == 'Nov' & day <= 15, 1, 0)) %>%
  mutate(Nov16_30 = if_else(mo == 'Nov' & day > 15, 1, 0)) %>%
  mutate(Dec1_15 = if_else(mo == 'Dec' & day <= 15, 1, 0)) %>%
  mutate(Dec16_31 = if_else(mo == 'Dec' & day > 15, 1, 0))

# Calculate the number of tides in each half-month category
ntide = aggregate(td[c('Jan1_15','Jan16_31','Feb1_15','Feb16_28','Mar1_15','Mar16_31',
                       'Apr1_15','Apr16_30','May1_15','May16_31','Jun1_15','Jun16_30',
                       'Jul1_15','Jul16_31','Aug1_15','Aug16_31','Sep1_15','Sep16_30',
                       'Oct1_15','Oct16_31','Nov1_15','Nov16_30','Dec1_15','Dec16_31')],
                       by = list(td$s), sum)
names(ntide) = c('Strata','Jan1_15','Jan16_31','Feb1_15','Feb16_28','Mar1_15','Mar16_31',
                 'Apr1_15','Apr16_30','May1_15','May16_31','Jun1_15','Jun16_30','Jul1_15',
                 'Jul16_31','Aug1_15','Aug16_31','Sep1_15','Sep16_30','Oct1_15','Oct16_31',
                 'Nov1_15','Nov16_30','Dec1_15','Dec16_31')

#===========================================================
# Final data output ----
#===========================================================

# Combine tide data with clam and oyster data
CPro = cp %>%
  left_join(ntide, by = "Strata") %>%
  arrange(BIDN, Strata)

OPro = op %>%
  left_join(ntide, by = "Strata") %>%
  arrange(BIDN, Strata)

# Output CPro to excel
num_cols = ncol(CPro)
out_name = glue("Summary/SeasonProjection/data/{current_year}_CPro.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "CPro", gridLines = TRUE)
writeData(wb, sheet = 1, CPro, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

# Output OPro to excel
num_cols = ncol(OPro)
out_name = glue("Summary/SeasonProjection/data/{current_year}_OPro.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "OPro", gridLines = TRUE)
writeData(wb, sheet = 1, OPro, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

# Output Cpue to excel
num_cols = ncol(Cpue)
out_name = glue("Summary/SeasonProjection/data/{current_year}_Cpue.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "Cpue", gridLines = TRUE)
writeData(wb, sheet = 1, Cpue, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)

# Output Opue to excel
num_cols = ncol(Opue)
out_name = glue("Summary/SeasonProjection/data/{current_year}_Opue.xlsx")
wb <- createWorkbook(out_name)
addWorksheet(wb, "Opue", gridLines = TRUE)
writeData(wb, sheet = 1, Opue, rowNames = FALSE)
## create and add a style to the column headers
headerStyle <- createStyle(fontSize = 12, fontColour = "#070707", halign = "left",
                           fgFill = "#C8C8C8", border="TopBottom", borderColour = "#070707")
addStyle(wb, sheet = 1, headerStyle, rows = 1, cols = 1:num_cols, gridExpand = TRUE)
saveWorkbook(wb, out_name, overwrite = TRUE)












