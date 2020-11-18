
-- Started writing on 2017-10-23

-- Consider how to identify ground count only beaches
-- Or special treatment beaches. May be able to use
-- beach_status_id in beach_allowance table
-- Create tide_strata view, with P, and S as substr from name
-- Use power_user account for DBs on local

-- ToDo:
-- 1. Add survey_type_code to survey_type_lut...DONE
-- 2. Dump sfma and mng-region from point_location table
--    If areas change in the future data will be incorrect
--    Better to just use spatial join to get values...DONE
-- 3. Restructure any tables with geoms to add gid...DONE
-- 4. Convert point_location coordinates, geom to NOT NULL...DONE
-- 5. Convert pgcrypto uuid...DONE

-- Create extensions (Query window on database shellfish) -------------------

COMMENT ON DATABASE shellfish
    IS 'Database for Puget Sound Shellfish Unit';
    
--GRANT CREATE, CONNECT ON DATABASE shellfish TO stromas;
--GRANT TEMPORARY ON DATABASE shellfish TO stromas WITH GRANT OPTION;

--CREATE EXTENSION postgis
--    SCHEMA public
--    VERSION "2.3.3";
    
--CREATE EXTENSION "pgcryto"
--    SCHEMA public
--    VERSION "1.3";
    
-- Manually grant all on public to stromas after creating tables below...generates many lines.

-- Create tables (Query window on database shellfish) -------------------

CREATE TABLE area_surveyed_lut (
    area_surveyed_id uuid DEFAULT gen_random_uuid() NOT NULL,
    area_surveyed_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE beach (
    beach_id uuid DEFAULT gen_random_uuid() NOT NULL,
    tide_station_location_id uuid NOT NULL,
    local_beach_name character varying(50), 
    beach_description character varying(250),
    low_tide_correction_minutes integer NOT NULL,
    low_tide_correction_feet numeric(4,2),
    high_tide_correction_minutes integer,
    high_tide_correction_feet numeric(4,2),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE beach_allowance (
    beach_allowance_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    beach_status_id uuid NOT NULL,
    effort_estimate_type_id uuid NOT NULL,
    egress_model_type_id uuid NOT NULL,
    species_group_id uuid NOT NULL,
    report_type_id uuid NOT NULL,
    harvest_unit_type_id uuid,
    allowance_year integer NOT NULL,
    allowable_harvest integer,
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE beach_boundary_history (
    beach_boundary_history_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    survey_type_id uuid NOT NULL,
    beach_number integer NOT NULL,
    beach_name text NOT NULL,
    active_indicator boolean NOT NULL,
    active_datetime timestamptz(6),
    inactive_datetime timestamptz(6),
    inactive_reason text,
    gid integer NOT NULL,
    geom geometry(multipolygon, 2927),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE beach_intertidal_area (
    beach_intertidal_area_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    gid integer NOT NULL,
    geom geometry(multipolygon, 2927),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE beach_inventory (
    beach_inventory_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    survey_type_id uuid NOT NULL, 
    species_id uuid NOT NULL,
    population_estimate_type uuid NOT NULL,
    population_estimate_unit uuid NOT NULL,
    survey_completed_datetime timestamptz NOT NULL,
    population_estimate integer NOT NULL,
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE beach_season (
    beach_season_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    season_status_id uuid NOT NULL,
    species_group_id uuid NOT NULL,
    season_start_datetime timestamptz(6) NOT NULL,
    season_end_datetime timestamptz(6) NOT NULL,
    season_description character varying(100),
    comment_text character varying(200),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE beach_status_lut (
    beach_status_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_status_code character varying(20) NOT NULL,
    beach_status_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE catch_result_type_lut (
    catch_result_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    catch_result_type_code character varying(20) NOT NULL,
    catch_result_type_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE data_review_status_lut (
    data_review_status_id uuid DEFAULT gen_random_uuid() NOT NULL,
    data_review_status_description character varying(25) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE effort_estimate_type_lut (
    effort_estimate_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    effort_estimate_type_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE egress_model_version (
    egress_model_version_id uuid DEFAULT gen_random_uuid() NOT NULL,
    egress_model_type_id uuid NOT NULL, 
    tide_strata_id uuid NOT NULL,
    egress_model_name character varying(50) NOT NULL,
    egress_model_description character varying (250) NOT NULL,
    egress_model_source character varying (50),
    inactive_indicator boolean NOT NULL, 
    inactive_datetime timestamptz(6),
    inactive_reason character varying(100),
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE egress_model (
    egress_model_id uuid DEFAULT gen_random_uuid() NOT NULL,
    egress_model_version_id uuid NOT NULL,
    egress_model_interval integer NOT NULL,
    egress_model_ratio numeric(7,6),
    egress_model_variance numeric(8,7),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE egress_model_type_lut (
    egress_model_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    egress_model_type_code character varying(10) NOT NULL,
    egress_model_type_description character varying(100) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE harvest_depth_range_lut (
    harvest_depth_range_id uuid DEFAULT gen_random_uuid() NOT NULL,
    harvest_depth_range character varying(20) NOT NULL,
    depth_range_description character varying(50) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE harvest_gear_type_lut (
    harvest_gear_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    harvest_gear_type_code character varying(25) NOT NULL,
    gear_type_description character varying(250) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE harvest_method_lut (
    harvest_method_id uuid DEFAULT gen_random_uuid() NOT NULL,
    harvest_method_code character varying(25) NOT NULL,
    harvest_method_description character varying(250) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE harvest_unit_type_lut (
    harvest_unit_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    harvest_unit_type_code character varying(20) NOT NULL,
    harvest_unit_type_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE harvester_type_lut (
    harvester_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    harvester_type_code character varying(25) NOT NULL,
    harvester_type_description character varying(250) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE individual_species (
    individual_species_id uuid DEFAULT gen_random_uuid() NOT NULL,
    species_encounter_id uuid NOT NULL,
    sex_id uuid NOT NULL,
    species_sample_number character varying(40),
    weight_measurement_gram numeric(8,2),
    length_measurement_millimeter numeric(8,2),
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE location_type_lut (
    location_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    location_type_description character varying(50) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE management_region_lut (
    management_region_id uuid DEFAULT gen_random_uuid() NOT NULL,
    management_region_code integer NOT NULL,
    management_region_description text,
    gid integer NOT NULL,
    geom geometry(polygon, 2927),
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

-- Final mean estimate tables for use in projections....These may be temparary

CREATE TABLE mean_effort_estimate (
    mean_effort_estimate_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    beach_number integer NOT NULL,
    beach_name text NOT NULL,
    estimation_year integer NOT NULL,
    tide_strata character varying(8) NOT NULL,
    flight_season character varying(8) NOT NULL,
    mean_effort numeric(18,14) NOT NULL,
    tide_count integer NOT NULL,
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE mean_cpue_estimate (
    mean_cpue_estimate_id uuid DEFAULT gen_random_uuid() NOT NULL,
    beach_id uuid NOT NULL,
    beach_number integer NOT NULL,
    beach_name text NOT NULL,
    estimation_year integer NOT NULL,
    flight_season character varying(8) NOT NULL,
    species_code character varying(8) NOT NULL,
    survey_count integer NOT NULL,
    mean_cpue numeric(18,14) NOT NULL,
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

----------------------------------------------------- End

CREATE TABLE mobile_device (
    mobile_device_id uuid DEFAULT gen_random_uuid() NOT NULL,
    mobile_device_type_id uuid NOT NULL,
    mobile_equipment_identifier character varying(15) NOT NULL,
    mobile_device_name character varying(50) NOT NULL,
    mobile_device_description character varying(150) NOT NULL,
    active_indicator boolean NOT NULL,
    inactive_datetime timestamptz(6),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE mobile_device_type_lut (
    mobile_device_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    mobile_device_type_description character varying(50) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE mobile_survey_form (
    mobile_survey_form_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_id uuid NOT NULL,
    parent_form_survey_id integer NOT NULL,
    parent_form_survey_guid uuid,
    parent_form_name character varying(100),
    parent_form_id character varying(40),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE point_location (
    point_location_id uuid DEFAULT gen_random_uuid() NOT NULL,
    location_type_id uuid NOT NULL,
    beach_id uuid,
    location_code text,
    location_name text,
    location_description text,
    horizontal_accuracy numeric(8,2),
    comment_text character varying(1000),
    gid integer NOT NULL,
    geom geometry(point, 2927) NOT NULL,
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE report_type_lut (
    report_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    report_type_code character varying(20) NOT NULL,
    report_type_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE sampler (
    sampler_id uuid DEFAULT gen_random_uuid() NOT NULL,
    first_name character varying(50) NOT NULL,
    last_name character varying(50) NOT NULL,
    active_indicator boolean NOT NULL,
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE sampling_program_lut (
    sampling_program_id uuid DEFAULT gen_random_uuid() NOT NULL,
    agency_id uuid NOT NULL,
    sampling_program_code character varying(10) NOT NULL,
    sampling_program_name character varying(100) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE season_status_lut (
    season_status_id uuid DEFAULT gen_random_uuid() NOT NULL,
    season_status_code character varying(2) NOT NULL,
    season_status_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE season_type_lut (
    season_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    season_type_code character varying(1) NOT NULL,
    season_type_description character varying(25) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE sex_lut (
    sex_id uuid DEFAULT gen_random_uuid() NOT NULL,
    sex_code character varying(2) NOT NULL,
    sex_description character varying(25) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE shell_condition_lut (
    shell_condition_id uuid DEFAULT gen_random_uuid() NOT NULL,
    shell_condition_code character varying(20) NOT NULL,
    shell_condition_description character varying(200) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE shellfish_management_area_lut (
    shellfish_management_area_id uuid DEFAULT gen_random_uuid() NOT NULL,
    shellfish_area_code character varying(3) NOT NULL,
    shellfish_area_description character varying(200),
    gid integer NOT NULL,
    geom geometry(polygon, 2927),
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE species_encounter (
    species_encounter_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_event_id uuid NOT NULL,
    species_id uuid NOT NULL,
    species_location_id uuid,
    catch_result_type_id uuid NOT NULL,
    shell_condition_id uuid NOT NULL,
    species_count integer NOT NULL,
    species_weight_gram numeric(8,2),
    no_head_indicator boolean NOT NULL, 
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE species_group_lut (
    species_group_id uuid DEFAULT gen_random_uuid() NOT NULL,
    species_group_code character varying(10) NOT NULL,
    species_group_description character varying(50) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE species_lut (
    species_id uuid DEFAULT gen_random_uuid() NOT NULL,
    taxo_name_id integer,
    species_code character varying(25) NOT NULL,
    common_name character varying(100) NOT NULL,
    genus character varying(50),
    species character varying(50),
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE survey (
    survey_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_type_id uuid NOT NULL,
    sampling_program_id uuid NOT NULL,
    beach_id uuid,
    point_location_id uuid,
    area_surveyed_id uuid, 
    data_review_status_id uuid NOT NULL,
    survey_completion_status_id uuid NOT NULL,
    survey_datetime timestamptz(6) NOT NULL,
    start_datetime timestamptz(6),
    end_datetime timestamptz(6),
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE survey_completion_status_lut (
    survey_completion_status_id uuid DEFAULT gen_random_uuid() NOT NULL,
	  completion_status_description varchar(100) NOT NULL,
	  obsolete_flag boolean NOT NULL,
	  obsolete_datetime timestamptz(6)
);

CREATE TABLE survey_event (
    survey_event_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_id uuid NOT NULL,
    event_location_id uuid,
    harvester_type_id uuid NOT NULL,
    harvest_method_id uuid NOT NULL,
    harvest_gear_type_id uuid NOT NULL,
    harvest_depth_range_id uuid NOT NULL,
    event_number integer,
    event_datetime timestamptz(6),
    harvester_count integer,
    harvest_gear_count integer,
    harvester_zip_code character varying(12),
    comment_text character varying(1000),
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE survey_mobile_device (
    survey_mobile_device_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_id uuid NOT NULL,
    mobile_device_id uuid NOT NULL
);

CREATE TABLE survey_sampler (
    survey_sampler_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_id uuid NOT NULL,
    sampler_id uuid
);

CREATE TABLE survey_type_lut (
    survey_type_id uuid DEFAULT gen_random_uuid() NOT NULL,
    survey_type_code character varying(25) NOT NULL,
    survey_type_description character varying(150) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

CREATE TABLE tide (
    tide_id uuid DEFAULT gen_random_uuid() NOT NULL,
    tide_strata_id uuid NOT NULL,
    tide_station_location_id uuid NOT NULL,
    low_tide_datetime timestamptz(6) NOT NULL,
    tide_time_minutes integer NOT NULL,
    tide_height_feet numeric(4,2) NOT NULL,
    created_datetime timestamptz(6) NOT NULL,
    created_by text NOT NULL,
    modified_datetime timestamptz(6),
    modified_by text
);

CREATE TABLE tide_strata_lut (
    tide_strata_id uuid DEFAULT gen_random_uuid() NOT NULL,
    tide_strata_code character varying(10) NOT NULL,
    tide_strata_description character varying(150) NOT NULL,
    obsolete_flag boolean NOT NULL,
    obsolete_datetime timestamptz(6)
);

-- Set primary keys ------------------------------------------------------
    
ALTER TABLE ONLY area_surveyed_lut
    ADD CONSTRAINT pk_area_surveyed_lut PRIMARY KEY (area_surveyed_id);

ALTER TABLE ONLY beach
    ADD CONSTRAINT pk_beach PRIMARY KEY (beach_id);

ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT pk_beach_allowance PRIMARY KEY (beach_allowance_id);
    
ALTER TABLE ONLY beach_boundary_history
    ADD CONSTRAINT pk_beach_boundary_history PRIMARY KEY (beach_boundary_history_id);
    
ALTER TABLE ONLY beach_intertidal_area
    ADD CONSTRAINT pk_beach_intertidal_area PRIMARY KEY (beach_intertidal_area_id);
    
ALTER TABLE ONLY beach_inventory
    ADD CONSTRAINT pk_beach_inventory PRIMARY KEY (beach_inventory_id);

ALTER TABLE ONLY beach_season
    ADD CONSTRAINT pk_beach_season PRIMARY KEY (beach_season_id);

ALTER TABLE ONLY beach_status_lut
    ADD CONSTRAINT pk_beach_status_lut PRIMARY KEY (beach_status_id);

ALTER TABLE ONLY catch_result_type_lut
    ADD CONSTRAINT pk_catch_result_type_lut PRIMARY KEY (catch_result_type_id);

ALTER TABLE ONLY data_review_status_lut
    ADD CONSTRAINT pk_data_review_status_lut PRIMARY KEY (data_review_status_id);
    
ALTER TABLE ONLY effort_estimate_type_lut
    ADD CONSTRAINT pk_effort_estimate_type_lut PRIMARY KEY (effort_estimate_type_id);
    
ALTER TABLE ONLY egress_model
    ADD CONSTRAINT pk_egress_model PRIMARY KEY (egress_model_id);
    
ALTER TABLE ONLY egress_model_type_lut
    ADD CONSTRAINT pk_egress_model_type_lut PRIMARY KEY (egress_model_type_id);
    
ALTER TABLE ONLY egress_model_version
    ADD CONSTRAINT pk_egress_model_version PRIMARY KEY (egress_model_version_id);

ALTER TABLE ONLY harvest_depth_range_lut
    ADD CONSTRAINT pk_harvest_depth_range_lut PRIMARY KEY (harvest_depth_range_id);

ALTER TABLE ONLY harvest_gear_type_lut
    ADD CONSTRAINT pk_harvest_gear_type_lut PRIMARY KEY (harvest_gear_type_id);

ALTER TABLE ONLY harvest_method_lut
    ADD CONSTRAINT pk_harvest_method_lut PRIMARY KEY (harvest_method_id);
    
ALTER TABLE ONLY harvest_unit_type_lut
    ADD CONSTRAINT pk_harvest_unit_type_lut PRIMARY KEY (harvest_unit_type_id);

ALTER TABLE ONLY harvester_type_lut
    ADD CONSTRAINT pk_harvester_type_lut PRIMARY KEY (harvester_type_id);

ALTER TABLE ONLY individual_species
    ADD CONSTRAINT pk_individual_species PRIMARY KEY (individual_species_id);
    
ALTER TABLE ONLY location_type_lut
    ADD CONSTRAINT pk_location_type_lut PRIMARY KEY (location_type_id);
    
ALTER TABLE ONLY management_region_lut
    ADD CONSTRAINT pk_management_region_lut PRIMARY KEY (management_region_id);
    
ALTER TABLE ONLY mobile_device
    ADD CONSTRAINT pk_mobile_device PRIMARY KEY (mobile_device_id);
    
ALTER TABLE ONLY mean_effort_estimate
    ADD CONSTRAINT pk_mean_effort_estimate_id PRIMARY KEY (mean_effort_estimate_id);
    
ALTER TABLE ONLY mean_cpue_estimate
    ADD CONSTRAINT pk_mean_cpue_estimate_id PRIMARY KEY (mean_cpue_estimate_id);

ALTER TABLE ONLY mobile_device_type_lut
    ADD CONSTRAINT pk_mobile_device_type_lut PRIMARY KEY (mobile_device_type_id);
    
ALTER TABLE ONLY mobile_survey_form
    ADD CONSTRAINT pk_mobile_survey_form PRIMARY KEY (mobile_survey_form_id);

ALTER TABLE ONLY point_location
    ADD CONSTRAINT pk_point_location PRIMARY KEY (point_location_id);
    
ALTER TABLE ONLY report_type_lut
    ADD CONSTRAINT pk_report_type_lut PRIMARY KEY (report_type_id);
    
ALTER TABLE ONLY sampler
    ADD CONSTRAINT pk_sampler PRIMARY KEY (sampler_id);
    
ALTER TABLE ONLY sampling_program_lut
   ADD CONSTRAINT pk_sampling_program_lut PRIMARY KEY (sampling_program_id);

ALTER TABLE ONLY season_status_lut
    ADD CONSTRAINT pk_season_status_lut PRIMARY KEY (season_status_id);
    
ALTER TABLE ONLY season_type_lut
    ADD CONSTRAINT pk_season_type_lut PRIMARY KEY (season_type_id);
    
ALTER TABLE ONLY sex_lut
    ADD CONSTRAINT pk_sex_lut PRIMARY KEY (sex_id);

ALTER TABLE ONLY shell_condition_lut
    ADD CONSTRAINT pk_shell_condition_lut PRIMARY KEY (shell_condition_id);

ALTER TABLE ONLY shellfish_management_area_lut
    ADD CONSTRAINT pk_shellfish_management_area_lut PRIMARY KEY (shellfish_management_area_id);

ALTER TABLE ONLY species_encounter
    ADD CONSTRAINT pk_species_encounter PRIMARY KEY (species_encounter_id);

ALTER TABLE ONLY species_group_lut
    ADD CONSTRAINT pk_species_group_lut PRIMARY KEY (species_group_id);
    
ALTER TABLE ONLY species_lut
    ADD CONSTRAINT pk_species_lut PRIMARY KEY (species_id);
    
ALTER TABLE ONLY survey
    ADD CONSTRAINT pk_survey PRIMARY KEY (survey_id);
    
ALTER TABLE ONLY survey_completion_status_lut
    ADD CONSTRAINT pk_survey_completion_status_lut PRIMARY KEY (survey_completion_status_id);

ALTER TABLE ONLY survey_event
    ADD CONSTRAINT pk_survey_event PRIMARY KEY (survey_event_id);

ALTER TABLE ONLY survey_mobile_device
    ADD CONSTRAINT pk_survey_mobile_device PRIMARY KEY (survey_mobile_device_id);

ALTER TABLE ONLY survey_sampler
    ADD CONSTRAINT pk_survey_sampler PRIMARY KEY (survey_sampler_id);

ALTER TABLE ONLY survey_type_lut
    ADD CONSTRAINT pk_survey_type_lut PRIMARY KEY (survey_type_id);
    
ALTER TABLE ONLY tide
    ADD CONSTRAINT pk_tide PRIMARY KEY (tide_id);
    
ALTER TABLE ONLY tide_strata_lut
    ADD CONSTRAINT pk_tide_strata_lut PRIMARY KEY (tide_strata_id);

-- Set foreign keys ------------------------------------------------------
    
ALTER TABLE ONLY beach
    ADD CONSTRAINT fk_point_location__beach FOREIGN KEY (tide_station_location_id) REFERENCES point_location(point_location_id);

ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_beach__beach_allowance FOREIGN KEY (beach_id) REFERENCES beach(beach_id);
    
ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_beach_status_lut__beach_allowance FOREIGN KEY (beach_status_id) REFERENCES beach_status_lut(beach_status_id);
    
ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_effort_estimate_type_lut__beach_allowance FOREIGN KEY (effort_estimate_type_id) REFERENCES effort_estimate_type_lut(effort_estimate_type_id);
    
ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_egress_model_type_lut__beach_allowance FOREIGN KEY (egress_model_type_id) REFERENCES egress_model_type_lut(egress_model_type_id);
    
ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_species_group_lut__beach_allowance FOREIGN KEY (species_group_id) REFERENCES species_group_lut(species_group_id);
    
ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_report_type_lut__beach_allowance FOREIGN KEY (report_type_id) REFERENCES report_type_lut(report_type_id);
    
ALTER TABLE ONLY beach_allowance
    ADD CONSTRAINT fk_harvest_unit_type_lut__beach_allowance FOREIGN KEY (harvest_unit_type_id) REFERENCES harvest_unit_type_lut(harvest_unit_type_id);
    
ALTER TABLE ONLY beach_boundary_history
    ADD CONSTRAINT fk_beach__beach_boundary_history FOREIGN KEY (beach_id) REFERENCES beach(beach_id);

ALTER TABLE ONLY beach_boundary_history
    ADD CONSTRAINT fk_survey_type_lut__beach_boundary_history FOREIGN KEY (survey_type_id) REFERENCES survey_type_lut(survey_type_id);
    
ALTER TABLE ONLY beach_intertidal_area
    ADD CONSTRAINT fk_beach__beach_intertidal_area FOREIGN KEY (beach_id) REFERENCES beach(beach_id);
    
ALTER TABLE ONLY beach_inventory
    ADD CONSTRAINT fk_beach__beach_inventory FOREIGN KEY (beach_id) REFERENCES beach(beach_id);
    
ALTER TABLE ONLY beach_inventory
    ADD CONSTRAINT fk_survey_type_lut__beach_inventory FOREIGN KEY (survey_type_id) REFERENCES survey_type_lut(survey_type_id);
    
ALTER TABLE ONLY beach_inventory
    ADD CONSTRAINT fk_species_lut__beach_inventory FOREIGN KEY (species_id) REFERENCES species_lut(species_id);

ALTER TABLE ONLY beach_season
    ADD CONSTRAINT fk_beach__beach_season FOREIGN KEY (beach_id) REFERENCES beach(beach_id);
    
ALTER TABLE ONLY beach_season
    ADD CONSTRAINT fk_season_status_lut__beach_season FOREIGN KEY (season_status_id) REFERENCES season_status_lut(season_status_id);
    
ALTER TABLE ONLY beach_season
    ADD CONSTRAINT fk_species_group_lut__beach_season FOREIGN KEY (species_group_id) REFERENCES species_group_lut(species_group_id);

ALTER TABLE ONLY egress_model
    ADD CONSTRAINT fk_egress_model_version__egress_model FOREIGN KEY (egress_model_version_id) REFERENCES egress_model_version(egress_model_version_id);

ALTER TABLE ONLY egress_model_version
    ADD CONSTRAINT fk_egress_model_type_lut__egress_model_version FOREIGN KEY (egress_model_type_id) REFERENCES egress_model_type_lut(egress_model_type_id);
    
ALTER TABLE ONLY egress_model_version
    ADD CONSTRAINT fk_tide_strata_lut__egress_model_version FOREIGN KEY (tide_strata_id) REFERENCES tide_strata_lut(tide_strata_id);

ALTER TABLE ONLY individual_species
    ADD CONSTRAINT fk_species_encounter__individual_species FOREIGN KEY (species_encounter_id) REFERENCES species_encounter(species_encounter_id);
    
ALTER TABLE ONLY individual_species
    ADD CONSTRAINT fk_sex_lut__individual_species FOREIGN KEY (sex_id) REFERENCES sex_lut(sex_id);
    
ALTER TABLE ONLY mobile_device
    ADD CONSTRAINT fk_mobile_device_type_lut__mobile_device FOREIGN KEY (mobile_device_type_id) REFERENCES mobile_device_type_lut(mobile_device_type_id);

ALTER TABLE ONLY mobile_survey_form
    ADD CONSTRAINT fk_survey__mobile_survey_form FOREIGN KEY (survey_id) REFERENCES survey(survey_id);
    
ALTER TABLE ONLY point_location
    ADD CONSTRAINT fk_beach__point_location FOREIGN KEY (beach_id) REFERENCES beach(beach_id);

ALTER TABLE ONLY point_location
    ADD CONSTRAINT fk_location_type_lut__point_location FOREIGN KEY (location_type_id) REFERENCES location_type_lut(location_type_id);

ALTER TABLE ONLY species_encounter
    ADD CONSTRAINT fk_survey_event__species_encounter FOREIGN KEY (survey_event_id) REFERENCES survey_event(survey_event_id);
    
ALTER TABLE ONLY species_encounter
    ADD CONSTRAINT fk_species_lut__species_encounter FOREIGN KEY (species_id) REFERENCES species_lut(species_id);
    
ALTER TABLE ONLY species_encounter
    ADD CONSTRAINT fk_point_location__species_encounter FOREIGN KEY (species_location_id) REFERENCES point_location(point_location_id);
    
ALTER TABLE ONLY species_encounter
    ADD CONSTRAINT fk_catch_result_type_lut__species_encounter FOREIGN KEY (catch_result_type_id) REFERENCES catch_result_type_lut(catch_result_type_id);
    
ALTER TABLE ONLY species_encounter
    ADD CONSTRAINT fk_shell_condition_lut__species_encounter FOREIGN KEY (shell_condition_id) REFERENCES shell_condition_lut(shell_condition_id);

ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_survey_type_lut__survey FOREIGN KEY (survey_type_id) REFERENCES survey_type_lut(survey_type_id);

ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_sampling_program_lut__survey FOREIGN KEY (sampling_program_id) REFERENCES sampling_program_lut(sampling_program_id);

ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_beach__survey FOREIGN KEY (beach_id) REFERENCES beach(beach_id);
    
ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_point_location__survey FOREIGN KEY (point_location_id) REFERENCES point_location(point_location_id);
    
ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_area_surveyed_lut__survey FOREIGN KEY (area_surveyed_id) REFERENCES area_surveyed_lut(area_surveyed_id);

ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_data_review_status_lut__survey FOREIGN KEY (data_review_status_id) REFERENCES data_review_status_lut(data_review_status_id);
    
ALTER TABLE ONLY survey
    ADD CONSTRAINT fk_survey_completion_status_lut__survey FOREIGN KEY (survey_completion_status_id) REFERENCES survey_completion_status_lut(survey_completion_status_id);

ALTER TABLE ONLY survey_event
    ADD CONSTRAINT fk_survey__survey_event FOREIGN KEY (survey_id) REFERENCES survey(survey_id);

ALTER TABLE ONLY survey_event
    ADD CONSTRAINT fk_point_location__survey_event FOREIGN KEY (event_location_id) REFERENCES point_location(point_location_id);
    
ALTER TABLE ONLY survey_event
    ADD CONSTRAINT fk_harvester_type_lut__survey_event FOREIGN KEY (harvester_type_id) REFERENCES harvester_type_lut(harvester_type_id);

ALTER TABLE ONLY survey_event
    ADD CONSTRAINT fk_harvest_method_lut__survey_event FOREIGN KEY (harvest_method_id) REFERENCES harvest_method_lut(harvest_method_id);
    
ALTER TABLE ONLY survey_event
    ADD CONSTRAINT fk_harvest_gear_type_lut__survey_event FOREIGN KEY (harvest_gear_type_id) REFERENCES harvest_gear_type_lut(harvest_gear_type_id);
    
ALTER TABLE ONLY survey_event
    ADD CONSTRAINT fk_harvest_depth_range_lut__survey_event FOREIGN KEY (harvest_depth_range_id) REFERENCES harvest_depth_range_lut(harvest_depth_range_id);

ALTER TABLE ONLY survey_mobile_device
    ADD CONSTRAINT fk_survey__survey_mobile_device FOREIGN KEY (survey_id) REFERENCES survey(survey_id);

ALTER TABLE ONLY survey_mobile_device
    ADD CONSTRAINT fk_mobile_device__survey_mobile_device FOREIGN KEY (mobile_device_id) REFERENCES mobile_device(mobile_device_id);

ALTER TABLE ONLY survey_sampler
    ADD CONSTRAINT fk_survey__survey_sampler FOREIGN KEY (survey_id) REFERENCES survey(survey_id);

ALTER TABLE ONLY survey_sampler
    ADD CONSTRAINT fk_sampler__survey_sampler FOREIGN KEY (sampler_id) REFERENCES sampler(sampler_id);

ALTER TABLE ONLY tide
    ADD CONSTRAINT fk_tide_strata_lut__tide FOREIGN KEY (tide_strata_id) REFERENCES tide_strata_lut(tide_strata_id);
    
ALTER TABLE ONLY tide
    ADD CONSTRAINT fk_point_location__tide FOREIGN KEY (tide_station_location_id) REFERENCES point_location(point_location_id);
    
-- Add normal indexes

-- point_location
CREATE INDEX point_location_beach_idx ON point_location (beach_id);

-- survey
CREATE INDEX survey_beach_idx ON survey (beach_id);
CREATE INDEX survey_point_location_idx ON survey (point_location_id);
CREATE INDEX survey_survey_datetime_idx ON survey ( date(timezone('UTC', survey_datetime)) );

-- mobile_survey_form
CREATE INDEX mobile_survey_form_survey_idx ON mobile_survey_form (survey_id);

-- survey_event
CREATE INDEX survey_event_survey_idx ON survey_event (survey_id);
CREATE INDEX survey_event_event_location_idx ON survey_event (event_location_id);

-- species_encounter
CREATE INDEX species_encounter_survey_event_idx ON species_encounter (survey_event_id);
CREATE INDEX species_encounter_species_location_idx ON species_encounter (species_location_id);

-- individual_species
CREATE INDEX individual_species_species_encounter_idx ON individual_species (species_encounter_id);

-- beach
CREATE INDEX beach_tide_station_location_idx ON beach (tide_station_location_id);

-- beach_allowance
CREATE INDEX beach_allowance_beach_idx ON beach_allowance (beach_id);

-- beach_boundary_history
CREATE INDEX beach_boundary_history_beach_idx ON beach_boundary_history (beach_id);

-- beach_season
CREATE INDEX beach_season_beach_idx ON beach_season (beach_id);

-- Add geometry indexes

CREATE INDEX beach_boundary_history_gix ON beach_boundary_history USING GIST (geom);

CREATE INDEX beach_intertidal_area_gix ON beach_intertidal_area USING GIST (geom);

CREATE INDEX management_region_gix ON management_region_lut USING GIST (geom);

CREATE INDEX point_location_gix ON point_location USING GIST (geom);

CREATE INDEX shellfish_management_area_gix ON shellfish_management_area_lut USING GIST (geom);
