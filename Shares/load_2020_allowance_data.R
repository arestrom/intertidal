#=================================================================
# Load beach allowance data for 2020
#
# NOTES:
#  1. Looks ready to upload.
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
library(stringi)
library(lubridate)
library(openxlsx)

# Set options
options(digits=14)

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

#==============================================================================
# Get the data
#==============================================================================

# Import the beach data from shellfish DB
qry = glue::glue("select beach_id, beach_number as bidn, beach_name, active_datetime, inactive_datetime ",
                 "from beach_boundary_history")

# Run the query
db_con = pg_con_local(dbname = "shellfish")
beach = DBI::dbGetQuery(db_con, qry)
dbDisconnect(db_con)

# Get start and end years for beaches
beach = beach %>%
  mutate(start_yr = as.integer(substr(active_datetime, 1, 4))) %>%
  mutate(end_yr = as.integer(substr(inactive_datetime, 1, 4)))

# Get the seasons data
allow = read.xlsx(glue("Shares/data/{current_year}_2010_ClamOysterAllowables.xlsx"), sheet = "Allowables", detectDates = TRUE)

# Pull out current_year data
allow = allow %>%
  filter(allow_year == current_year)

#======================================================================================
# Process
#======================================================================================

# Get the beach data
bchyr = beach %>%
  filter(start_yr == current_year & end_yr == current_year) %>%
  select(beach_id, bidn) %>%
  distinct()

# Warn for duplicated bidns. No more dups when I get rid of beach_name
if (any(duplicated(bchyr$bidn))) {
  cat("\nWarning: Duplicated BIDNs. Investigate!\n\n")
}

# Add beach_id
allyr = allow %>%
  # Update Purdy Spit BIDN to 280645
  mutate(bidn = as.integer(bidn)) %>%
  mutate(bidn = if_else(bidn == 280640L, 280645L, bidn)) %>%
  select(bidn, beach_name, allow_year, beach_status, report_type, species_group,
         clam_allow = `allow_lbs(ManNat)`, oys_allow = `allow_num(Oyster)`,
         butt_allow = `allow_lbs(butters)`, cock_allow = `allow_lbs(cock)`,
         hor_allow = `allow_lbs(hor)`, var_allow = `allow_lbs(var)`,
         comment_text = Notes) %>%
  mutate(bidn = as.integer(bidn)) %>%
  left_join(bchyr, by = "bidn")

# Get bidns with no beach_id
no_bch_id = allyr %>%
  filter(is.na(beach_id)) %>%
  select(bidn, beach_name) %>%
  distinct()

# Get rid of Joemma Beach for now. Need a polygon before I can use
allyr = allyr %>%
  mutate(species_group = trimws(species_group)) %>%
  mutate(beach_status = trimws(beach_status)) %>%
  filter(!is.na(beach_id))

# Stop to inspect beach names
cat("\nStop for a second to inspect beach names!\n\n")

# Inspect species_groups: Result....Needed to correct trailing whitespace in Bactive.
# Fixed in xlsx and added code to trim whitespace
unique(allyr$species_group)
unique(allyr$beach_status)

# Pull out sections for seasons table
all_clam = allyr %>%
  filter(species_group %in% c("ManNat", "Both", "AllClams")) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = clam_allow, comment_text) %>%
  # Littleneck
  mutate(species_group_id = "f9341a0d-0659-421a-963e-a63ed25afba0") %>%
  # Pounds
  mutate(harvest_unit_type_id = "73683c44-3d97-4e75-af8c-39104988e116") %>%
  filter(!is.na(allowable_harvest))

#=====================================================================================
# Pull out each species group so separate group_ids (clam vs Oyster, etc)
# and harvest unit types (lbs vs count) can be applied separately to each group.
# Filter to only data in each group with actual shares and allowable harvest > 0.
# Then combine back together into full dataset. Splitting out makes this simpler
# and more explicit to my eyes.
#=====================================================================================

# Pull out sections for seasons table
all_oys = allyr %>%
  filter(species_group %in% c("Both", "Oyster")) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = oys_allow, comment_text) %>%
  # Oyster
  mutate(species_group_id = "379db4d3-ec40-4ebf-b588-6f87d68ec303") %>%
  # Count
  mutate(harvest_unit_type_id = "4c31105f-bc88-4d6e-a714-1382dcfab280") %>%
  filter(!is.na(allowable_harvest) & !allowable_harvest == 0L)

# Pull out sections for seasons table
all_ex = allyr %>%
  filter(is.na(clam_allow) & is.na(oys_allow) & is.na(butt_allow) &
           is.na(cock_allow) & is.na(hor_allow) & is.na(var_allow)) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = clam_allow, comment_text) %>%
  # Not applicable
  mutate(species_group_id = "d3a9e986-36f4-4967-b22b-c9de5c1b20ee") %>%
  # Not applicable
  mutate(harvest_unit_type_id = "e87c3c6a-7091-43f3-808b-38ece15d622d")

# Pull out sections for seasons table
all_man = allyr %>%
  filter(species_group %in% c("Manila")) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = clam_allow, comment_text) %>%
  # Manila
  mutate(species_group_id = "257a414f-5e0f-42c3-bc2e-d6ca25aae5f2") %>%
  # Pounds
  mutate(harvest_unit_type_id = "73683c44-3d97-4e75-af8c-39104988e116") %>%
  filter(!is.na(allowable_harvest))

# Pull out sections for seasons table
all_butt = allyr %>%
  filter(!is.na(butt_allow) & !butt_allow == 0L) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = butt_allow, comment_text) %>%
  # Butter
  mutate(species_group_id = "433a6742-83e0-41d9-91c2-5818248a16d0") %>%
  # Pounds
  mutate(harvest_unit_type_id = "73683c44-3d97-4e75-af8c-39104988e116") %>%
  filter(!is.na(allowable_harvest))

# Pull out sections for seasons table
all_cock = allyr %>%
  filter(!is.na(cock_allow) & !cock_allow == 0L) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = cock_allow, comment_text) %>%
  # Cockle
  mutate(species_group_id = "1dfe4de3-6cfa-4a46-ac3e-822b56435616") %>%
  # Pounds
  mutate(harvest_unit_type_id = "73683c44-3d97-4e75-af8c-39104988e116") %>%
  filter(!is.na(allowable_harvest))

# Pull out sections for seasons table
all_hor = allyr %>%
  filter(!is.na(hor_allow) & !hor_allow == 0L) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = hor_allow, comment_text) %>%
  # Horse
  mutate(species_group_id = "98e1816b-c78e-4f67-85a9-ad8d40426c5f") %>%
  # Pounds
  mutate(harvest_unit_type_id = "73683c44-3d97-4e75-af8c-39104988e116") %>%
  filter(!is.na(allowable_harvest))

# Pull out sections for seasons table
all_var = allyr %>%
  filter(!is.na(var_allow) & !var_allow == 0L) %>%
  select(beach_id, bidn, beach_name, beach_status, report_type, species_group,
         allow_year, allowable_harvest = var_allow, comment_text) %>%
  # Varnish
  mutate(species_group_id = "843ec6e4-eac6-4793-86c0-656e34b4157f") %>%
  # Pounds
  mutate(harvest_unit_type_id = "73683c44-3d97-4e75-af8c-39104988e116") %>%
  filter(!is.na(allowable_harvest))

# Combine into one dataset
allowance_year = rbind(all_clam, all_oys, all_ex, all_man, all_butt, all_cock,
                   all_hor, all_var)

# Inspect beach_status and report_type
unique(allowance_year$beach_status)
unique(allowance_year$report_type)
# unique(allowance_table$species_text)
# unique(allowance_table$report_type_text)

# Verify no missing beach_id or bidn
any(is.na(allowance_year$bidn))
any(is.na(allowance_year$beach_id))

# Check report type
unique(allowance_year$report_type)

#============================================================================================
# Add database lookup table values for needed categories (species_group, beach_status, etc)
# Define egress model type (will only change if model is updated). Generate report_type_id
#============================================================================================

# Add remaining columns
allowance_table = allowance_year %>%
  mutate(beach_allowance_id = remisc::get_uuid(nrow(allowance_year))) %>%
  mutate(report_type = trimws(report_type)) %>%
  mutate(beach_status = remisc::trim(beach_status)) %>%
  mutate(beach_status_id = recode(beach_status,
                                  "Cactive" = "586f5014-4fa9-4652-a289-314e1073df7c",
                                  "Bactive" = "586f5014-4fa9-4652-a289-314e1073df7c",
                                  "Bactive " = "586f5014-4fa9-4652-a289-314e1073df7c",
                                  "Oactive" = "586f5014-4fa9-4652-a289-314e1073df7c",
                                  "Mactive" = "586f5014-4fa9-4652-a289-314e1073df7c",
                                  "AllCactive" = "586f5014-4fa9-4652-a289-314e1073df7c",
                                  "Passive" = "15c0958a-aca3-4a27-9231-34a95d7bf7af",
                                  "SingleEntity" = "503f4d73-78f7-4ff1-9959-7b1edc0fa689",
                                  "Bait" = "51238740-dcd9-4caa-83dd-ad267e3996fa")) %>%
  # ADD POINT ESTIMATE HERE IF DESIRED.....
  mutate(effort_estimate_type_id = "9361f970-e1de-449a-ae32-3718ffa16fc7") %>%
  mutate(egress_model_type = case_when(bidn %in% c(270460) ~ "Twanoh",
                                       bidn %in% c(270201, 270286, 270480) ~ "High",
                                       bidn %in% c(250260, 250470, 270300,
                                                   270440, 270442, 270310) ~ "Early",
                                       !bidn %in% c(270460, 270201, 270286,
                                                    270480, 250260, 250470,
                                                    270300, 270440, 270442,
                                                    270310) ~ "Normal")) %>%
  mutate(egress_model_type_id = recode(egress_model_type,
                                       "Twanoh" = "4705a69c-2f6f-4f93-bc66-4afb972fb141",
                                       "High" = "1316c88d-74b3-4961-9117-2e7f3daa069e",
                                       "Early" = "738d3017-a713-413e-9517-1c7899af4a04",
                                       "Normal" = "d0554a22-806e-42b8-857c-fa97bfd812c2")) %>%
  mutate(species_text = recode(species_group_id,
                          "1dfe4de3-6cfa-4a46-ac3e-822b56435616" = "cockle",
                          "1ebc008e-10e7-4c9c-b2a6-3b69651d9710" = "both",
                          "257a414f-5e0f-42c3-bc2e-d6ca25aae5f2" = "manila",
                          "379db4d3-ec40-4ebf-b588-6f87d68ec303" = "oyster",
                          "433a6742-83e0-41d9-91c2-5818248a16d0" = "butter",
                          "65fcd0ba-d701-4ee1-9e1e-960bad82ece2" = "clam",
                          "843ec6e4-eac6-4793-86c0-656e34b4157f" = "varnish",
                          "98e1816b-c78e-4f67-85a9-ad8d40426c5f" = "horse",
                          "d3a9e986-36f4-4967-b22b-c9de5c1b20ee" = "na",
                          "f9341a0d-0659-421a-963e-a63ed25afba0" = "littleneck")) %>%
  mutate(report_type_text = case_when(report_type == "CY OH" &
                                        species_text %in% c("littleneck", "manila", "butter", "cockle",
                                                            "horse", "varnish") ~ "external_harvest",
                                      report_type == "CY OH" &
                                        species_text %in% c("oyster") ~ "internal_harvest",
                                      report_type == "CY OY" &
                                        species_text %in% c("littleneck", "manila", "butter", "cockle",
                                                            "horse", "varnish") ~ "external_harvest",
                                      report_type == "CY OY" &
                                        species_text %in% c("oyster") ~ "external_harvest",
                                      report_type == "CY" &
                                        species_text %in% c("littleneck", "manila", "butter", "cockle",
                                                            "horse", "varnish") ~ "external_harvest",
                                      report_type == "OY CH" &
                                        species_text %in% c("littleneck", "manila", "butter", "cockle",
                                                            "horse", "varnish") ~ "internal_harvest",
                                      report_type == "OY CH" &
                                        species_text %in% c("oyster") ~ "external_harvest",
                                      report_type == "OY" &
                                        species_text %in% c("oyster") ~ "external_harvest",
                                      report_type == "CH OH" &
                                        species_text %in% c("littleneck", "manila", "butter", "cockle",
                                                            "horse", "varnish", "na") ~ "internal_harvest",
                                      report_type == "CH OH" &
                                        species_text %in% c("oyster", "na") ~ "internal_harvest",
                                      report_type == "CH" &
                                        species_text %in% c("littleneck", "manila", "butter", "cockle",
                                                            "horse", "varnish", "na") ~ "internal_harvest",
                                      report_type == "Effort" ~ "external_effort",
                                      report_type =="Point" ~ "external_effort")) %>%
  mutate(report_type_id = recode(report_type_text,
                                 "external_harvest" = "f56290f3-6d38-4bae-bf7c-591601422b58",
                                 "external_effort" = "31fd90b3-8f66-463b-b546-7cac15894278",
                                 "internal_harvest" = "dc5c0309-f222-44fc-b5e3-8d0210ac6fdf")) %>%
  mutate(allowable_harvest = as.integer(round(allowable_harvest))) %>%
  mutate(created_datetime = with_tz(Sys.time(), "UTC")) %>%
  mutate(created_by = "stromas") %>%
  mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) %>%
  mutate(modified_by = NA_character_) %>%
  select(beach_allowance_id, beach_id, beach_status_id, effort_estimate_type_id,
         egress_model_type_id, species_group_id, report_type_id, harvest_unit_type_id,
         allowance_year = allow_year, allowable_harvest, comment_text, created_datetime,
         created_by, modified_datetime, modified_by)

# Check comments
unique(allowance_table$comment_text)
# Trim
allowance_table = allowance_table %>%
  mutate(comment_text = trimws(comment_text)) %>%
  mutate(comment_text = if_else(comment_text == "", NA_character_, comment_text))

#============================================================================================
# Upload to database
#============================================================================================

# # Write to shellfish
# db_con = pg_con_local(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "beach_allowance", allowance_table, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)
#
# # Write to shellfish_archive
# db_con = pg_con_prod(dbname = "shellfish")
# DBI::dbWriteTable(db_con, "beach_allowance", allowance_table, row.names = FALSE, append = TRUE)
# DBI::dbDisconnect(db_con)

















