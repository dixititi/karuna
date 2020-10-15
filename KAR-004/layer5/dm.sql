/*
CCDM DM mapping
Notes: Standard mapping to CCDM DM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dm_data AS (
				
                SELECT  dm."STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                        dm."USUBJID"::text AS usubjid,
                        null::numeric AS visitnum,
                        null::text AS visit,
                        --COALESCE("MinCreated", "RecordDate")::date AS dmdtc,
                        dm."DMDTC"::date AS dmdtc,
                        dm."BRTHDTC"::date AS brthdtc,
                        dm."AGE"::integer AS age,
                        dm."SEX"::text AS sex,
                        dm."RACE"::text AS race,
                        dm."ETHNIC"::text AS ethnicity,
                        dm."ARMCD"::text AS armcd,
                        dm."ARM"::text AS arm
                   FROM kar004_sdtm."DM" dm
				)           

SELECT 
        /*KEY (dm.studyid || '~' || dm.siteid || '~' || dm.usubjid)::text AS comprehendid, KEY*/
        dm.studyid::text AS studyid,
        dm.siteid::text AS siteid,
        dm.usubjid::text AS usubjid,
        dm.visitnum::numeric AS visitnum,
        dm.visit::text AS visit,
        dm.dmdtc::date AS dmdtc,
        dm.brthdtc::date AS brthdtc,
        dm.age::integer AS age,
        dm.sex::text AS sex,
        dm.race::text AS race,
        dm.ethnicity::text AS ethnicity,
        dm.armcd::text AS armcd,
        dm.arm::text AS arm
        /*KEY ,(dm.studyid || '~' || dm.siteid || '~' || dm.usubjid )::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM dm_data dm
JOIN included_subjects s ON (dm.studyid = s.studyid AND dm.siteid = s.siteid AND dm.usubjid = s.usubjid);