-- AS, 2020-11-16

-- Create the view. Make sure any columns referenced are in the view.
CREATE OR REPLACE VIEW public.beach_info_2020 AS
 SELECT 
    row_number() OVER () AS vid,
    bb.gid,
    bb.beach_boundary_history_id,
    bb.beach_id,
    bb.survey_type_id,
    bb.beach_number,
    bb.beach_name,
    bb.active_indicator,
    bb.active_datetime,
    bb.inactive_datetime,
    bb.inactive_reason,
    bb.created_datetime,
    bb.created_by, 
    bb.modified_datetime,
    bb.modified_by,
    bb.geom
   FROM beach_boundary_history AS bb
   WHERE date_part('year', bb.active_datetime) = 2020
 ORDER BY bb.gid;

ALTER TABLE public.beach_info_2020
    OWNER TO stromas;


-- DROP FUNCTION public.beach_info_2019_change() CASCADE;

CREATE FUNCTION public.beach_info_2020_change()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF 
AS $BODY$
	      
    BEGIN
      IF TG_OP = 'UPDATE' THEN
       
       UPDATE beach_boundary_history 
       SET 
         gid=NEW.gid,
         beach_boundary_history_id=NEW.beach_boundary_history_id, 
         beach_id=NEW.beach_id, 
         survey_type_id=NEW.survey_type_id,
         beach_number=NEW.beach_number, 
         beach_name=NEW.beach_name, 
         active_indicator=NEW.active_indicator,
         active_datetime=NEW.active_datetime, 
         inactive_datetime=NEW.inactive_datetime,
         inactive_reason=NEW.inactive_reason, 
         created_datetime=NEW.created_datetime,
         created_by=NEW.created_by, 
         modified_datetime=now(),
         modified_by=current_user,
         geom=NEW.geom
	    WHERE beach_boundary_history_id=OLD.beach_boundary_history_id;
      RETURN NEW;
       
    ELSIF TG_OP = 'DELETE' THEN
       DELETE FROM beach_boundary_history WHERE beach_boundary_history_id=OLD.beach_boundary_history_id;
       RETURN NULL;
    END IF;
    RETURN NEW;
  END;

$BODY$;

ALTER FUNCTION public.beach_info_2020_change()
    OWNER TO stromas;


-- Create the trigger
CREATE TRIGGER beach_info_2020_trig
    INSTEAD OF INSERT OR DELETE OR UPDATE 
    ON public.beach_info_2020
    FOR EACH ROW
    EXECUTE PROCEDURE public.beach_info_2020_change();
        
        
        
        
        
  