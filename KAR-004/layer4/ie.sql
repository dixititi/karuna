/*
CCDM IE mapping
Notes: Standard mapping to CCDM IE table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid From subject ),

     ie_data AS (select studyid, siteid,  usubjid, visitnum, visit, iedtc, ietestcd, ietest, iecat, iescat,                        
     row_number() OVER (PARTITION BY studyid, siteid, usubjid)::int AS ieseq from 	 			
               (SELECT  "STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
						"USUBJID"::text  AS usubjid,
                        "VISITNUM"::numeric AS visitnum,
                        "VISIT"::text AS visit,
                        "IEDTC"::date AS iedtc,
                        "IETESTCD"::text AS ietestcd,
                        "IETEST"::text AS ietest,
                        "IECAT"::text AS iecat,
                        null::text AS iescat
                from kar004_sdtm."IE"	ie
                 where "IEORRES" = 'Y') ie_sub			
				)
				
SELECT 
        /*KEY (ie.studyid || '~' || ie.siteid || '~' || ie.usubjid)::text AS comprehendid, KEY*/
        ie.studyid::text AS studyid,
        ie.siteid::text AS siteid,
        ie.usubjid::text AS usubjid,
        ie.visitnum::numeric AS visitnum,
        ie.visit::text AS visit,
        ie.iedtc::date AS iedtc,
        ie.ieseq::integer AS ieseq,
        ie.ietestcd::text AS ietestcd,
        ie.ietest::text AS ietest,
        ie.iecat::text AS iecat,
        ie.iescat::text AS iescat
        /*KEY , (ie.studyid || '~' || ie.siteid || '~' || ie.usubjid || '~' || ie.ieseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ie_data ie 
JOIN included_subjects s ON (ie.studyid = s.studyid AND ie.siteid = s.siteid AND ie.usubjid = s.usubjid);
