/*
CCDM SV mapping
Notes: Standard mapping to CCDM SV table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject ),

     sv_data AS (
                SELECT  "STUDYID"::text AS studyid,
                        reverse(substring(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid, 
                        "VISITNUM"::numeric AS visitnum,
                        "VISIT"::text AS visit,
                        1::int AS visitseq, /* defaulted to 1 - deprecated */
                        min("SVSTDTC")::date AS svstdtc,
                        max("SVENDTC")::date AS svendtc
                        FROM kar004_sdtm."SV" 
						group by 1,2,3,4,5,6),
	
	formdata_visits AS (SELECT DISTINCT fd.studyid,
                                    fd.siteid,
                                    fd.usubjid,
                                    99::numeric AS visitnum, -- will be updated by cleanup script
                                    fd.visit,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svstdtc,
                                    coalesce(datacollecteddate,dataentrydate)::date AS svendtc
                            FROM formdata fd
                            LEFT JOIN sv_data sd ON (fd.studyid = sd.studyid and fd.siteid = sd.siteid and fd.usubjid = sd.usubjid and fd.visit = sd.visit)
                            WHERE sd.studyid IS NULL
                        ),
						
	all_visits AS (
                        SELECT studyid,
                                siteid,
                                usubjid,
                                visitnum,
                                visit,
                                svstdtc,
                                svendtc
                        FROM sv_data
                        UNION ALL
                        SELECT studyid,
                                siteid,
                                usubjid,
                                visitnum,
                                visit,
                                min(svstdtc) as svstdtc,
                                max(svendtc) as svendtc
                        FROM formdata_visits
                        group by studyid, siteid, usubjid, visitnum,visit
                        )
						

SELECT 
        /*KEY (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid)::text AS comprehendid, KEY*/
        sv.studyid::text AS studyid,
        sv.siteid::text AS siteid,
        sv.usubjid::text AS usubjid, 
        sv.visitnum::numeric AS visitnum,
        sv.visit::text AS visit,
        1::int  AS visitseq,
        sv.svstdtc::date AS svstdtc,
        sv.svendtc::date AS svendtc
        /*KEY , (sv.studyid || '~' || sv.siteid || '~' || sv.usubjid || '~' || sv.visit)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM all_visits sv
JOIN included_subjects s ON (sv.studyid = s.studyid AND sv.siteid = s.siteid AND sv.usubjid = s.usubjid);