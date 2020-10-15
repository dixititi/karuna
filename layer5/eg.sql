/*
CCDM EG mapping
Notes: Standard mapping to CCDM EG table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),
 eg_data AS 
(        
       SELECT "STUDYID"::text AS studyid, 
              reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
		      "USUBJID"::text AS usubjid,
               NULL::int AS egseq, 
               "EGTESTCD"::text AS egtestcd, 
                "EGTEST"::text AS egtest, 
                'ECG'::text AS egcat, 
                'ECG'::text AS egscat, 
                "EGPOS"::text AS egpos, 
                "EGORRES"::text AS egorres, 
                "EGORRESU"::text AS egorresu, 
				"EGSTRESN"::text AS egstresn,
				"EGSTRESU"::text AS egstresu, 
                "EGSTAT"::text AS egstat, 
                NULL::text AS egloc, 
				"EGBLFL"::text AS egblfl,
                "VISIT"::text AS visit,
                
                case when length(replace("EGDTC",'T',' '))=10 then concat("EGDTC",' 00:00:00')
                when length(replace("EGDTC",'T',' '))=16 then concat(replace("EGDTC",'T',' '),':00')
                end::text egdtc,
                       
		        case when length(replace("EGDTC",'T',' '))=10 then concat("EGDTC",' 00:00:00')
                when length(replace("EGDTC",'T',' '))=16 then concat(replace("EGDTC",'T',' '),':00')
                end::text AS egtm                
               
         FROM  kar004_sdtm."EG" 
          )
		  
SELECT 
       /*KEY (eg.studyid::text || '~' || eg.siteid::text || '~' || eg.usubjid::text) AS comprehendid, KEY*/
       eg.studyid::text                                   AS studyid, 
       eg.siteid::text                                    AS siteid, 
       eg.usubjid::text                                   AS usubjid, 
       eg.egseq::int                                      AS egseq, 
       eg.egtestcd::text                                  AS egtestcd, 
       eg.egtest::text                                    AS egtest, 
       eg.egcat::text                                     AS egcat, 
       eg.egscat::text                                    AS egscat, 
       eg.egpos::text                                     AS egpos, 
       eg.egorres::text                                   AS egorres, 
       eg.egorresu::text                                  AS egorresu, 
       eg.egstresn::numeric                               AS egstresn, 
       eg.egstresu::text                                  AS egstresu, 
       eg.egstat::text                                    AS egstat, 
       eg.egloc::text                                     AS egloc, 
	   eg.egblfl::text									  AS egblfl,
       eg.visit::text                                     AS visit, 
       eg.egdtc::timestamp without time zone              AS egdtc, 
       eg.egtm:: time without time zone                   AS egtm 
       /*KEY , (eg.studyid || '~' || eg.siteid || '~' || eg.usubjid || '~' || eg.egseq)::text AS objectuniquekey KEY*/
       /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/ 
FROM   eg_data eg 
JOIN   included_subjects s 
ON     (eg.studyid = s.studyid AND eg.siteid = s.siteid AND eg.usubjid = s.usubjid);