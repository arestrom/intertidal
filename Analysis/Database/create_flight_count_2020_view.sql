-- Code to create editable view for flight_counts 2020
-- Ok to run entire script, but delete objects first
-- AS 2020-11-16

create or replace view flight_count_2020 as
select 
  row_number() OVER() AS "vid",
  pl.gid, 
  se.survey_event_id,
  pl.point_location_id,
  se.event_datetime, 
  ht.harvester_type_code, 
  se.harvester_count,
  se.comment_text,
  pl.modified_datetime as point_modified_datetime,
  pl.modified_by as point_modified_by,
  se.modified_datetime as count_modified_datetime,
  se.modified_by as count_modified_by,
  pl.geom
from survey as s 
  left join survey_type_lut as st on s.survey_type_id = st.survey_type_id
  left join survey_event as se on s.survey_id = se.survey_id
  left join harvester_type_lut as ht on se.harvester_type_id = ht.harvester_type_id
  left join point_location as pl on se.event_location_id = pl.point_location_id
  where date_part('year', s.survey_datetime) = 2020  
  and st.survey_type_code = 'Clam aerial'
  order by vid;
  
ALTER TABLE public.flight_count_2020
    OWNER TO stromas;
    
-- DROP FUNCTION public.flight_count_2020_change() CASCADE;

CREATE FUNCTION public.flight_count_2020_change()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$
    
    BEGIN
      IF TG_OP = 'UPDATE' THEN
       UPDATE survey_event 
       SET 
		     event_datetime=NEW.event_datetime,
		     harvester_count=NEW.harvester_count,
		     modified_datetime=now(),
		     modified_by=current_user
	     WHERE survey_event_id=OLD.survey_event_id;
       UPDATE point_location 
       SET 
         geom=NEW.geom, 
         modified_datetime=now(), 
         modified_by=current_user
       WHERE point_location_id=OLD.point_location_id;
       RETURN NEW;
       
    ELSIF TG_OP = 'DELETE' THEN
       DELETE FROM survey_event WHERE survey_event_id=OLD.survey_event_id;
       DELETE FROM point_location WHERE point_location_id=OLD.point_location_id;
       RETURN NULL;
    END IF;
    RETURN NEW;
  END;
$BODY$;

ALTER FUNCTION public.flight_count_2020_change()
    OWNER TO stromas;

-- Create the trigger
CREATE TRIGGER flight_count_2020_trig
    INSTEAD OF DELETE OR UPDATE 
    ON public.flight_count_2020
    FOR EACH ROW
    EXECUTE PROCEDURE public.flight_count_2020_change();
       
       
       
       
       