/*
CCDM DV mapping
Notes: Standard mapping to CCDM DV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     dv_data AS (
			SELECT  
                        studyid,
                       siteid,
                       usubjid,
                       visit,
                        formid,
                        row_number() over (partition by dv.studyid, dv.siteid, dv.usubjid order by dv.dvstdtc,dv.dvterm)::int AS dvseq,
                        dvcat,
                        dvterm,
                       dvstdtc,
                        dvendtc,
                        dvscat,
                        dvid FROM
					 (SELECT  
                        "STUDYID"::text AS studyid,
                       reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                       "USUBJID"::text AS usubjid,
                        'Protocol Deviation'::text AS visit,
                        NULL::text AS formid,
                        case when length("DVCAT")=0 then 'No Category' ELSE "DVCAT" end  ::text AS dvcat,
                        "DVTERM"::text AS dvterm,
                        "DVSTDTC"::date AS dvstdtc,
                        "DVSTDTC"::date AS dvendtc,
                        "DVSCAT"::text AS dvscat,
                        NULL::text AS dvid 
					 FROM kar004_sdtm."DV") dv
                 )

SELECT 
        /*KEY (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid)::text AS comprehendid, KEY*/
        dv.studyid::text AS studyid,
        dv.siteid::text AS siteid,
        dv.usubjid::text AS usubjid,
        dv.visit::text AS visit,
        dv.formid::text AS formid,
        dv.dvseq::integer AS dvseq,
        dv.dvcat::text AS dvcat,
        dv.dvterm::text AS dvterm,
        dv.dvstdtc::date AS dvstdtc,
        dv.dvendtc::date AS dvendtc,
        dv.dvscat::text AS dvscat,
        dv.dvid::text AS dvid
        /*KEY , (dv.studyid || '~' || dv.siteid || '~' || dv.usubjid || '~' || dv.dvseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM dv_data dv
JOIN included_subjects s ON (dv.studyid = s.studyid AND dv.siteid = s.siteid AND dv.usubjid = s.usubjid);
