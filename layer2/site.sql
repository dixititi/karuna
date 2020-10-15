/*
CCDM Site mapping
Client:Karuna
Limits added for PR build efficiency (not applied for standard builds)
Notes: Standard mapping to CCDM Site table
*/

WITH included_studies AS (
                SELECT studyid FROM study ),

    site_data AS (
                SELECT  "STUDYID"::text AS studyid,
                        RIGHT("SITEID",3)::text AS siteid,
                        null::text AS sitename,
                        'Rave'::text AS croid,
                        'Rave'::text AS sitecro,
						null::text AS siteinvestigatorname,
						null::text AS sitecraname,
                        null::text AS sitecountry,
                        null::text AS siteregion,
                        null::date AS sitecreationdate,
                        null::date AS siteactivationdate,
                        null::date AS sitedeactivationdate,
                        null::text AS siteaddress1,
                        null::text AS siteaddress2,
                        null::text AS sitecity,
                        null::text AS sitestate,
                        null::text AS sitepostal,
                        null::text AS sitestatus,
                        null::date AS sitestatusdate
				From	kar004_sdtm."DM"
				/*LIMIT LIMIT 100 LIMIT*/)

SELECT 
        /*KEY (s.studyid || '~' || s.siteid)::text AS comprehendid, KEY*/
        s.studyid::text AS studyid,
        s.siteid::text AS siteid,
        s.sitename::text AS sitename,
        s.croid::text AS croid,
        s.sitecro::text AS sitecro,
        s.sitecountry::text AS sitecountry,
        s.siteregion::text AS siteregion,
        s.sitecreationdate::date AS sitecreationdate,
        s.siteactivationdate::date AS siteactivationdate,
        s.sitedeactivationdate::date AS sitedeactivationdate,
        s.siteinvestigatorname::text AS siteinvestigatorname,
        s.sitecraname::text AS sitecraname,
        s.siteaddress1::text AS siteaddress1,
        s.siteaddress2::text AS siteaddress2,
        s.sitecity::text AS sitecity,
        s.sitestate::text AS sitestate,
        s.sitepostal::text AS sitepostal,
        s.sitestatus::text AS sitestatus,
        s.sitestatusdate::date AS sitestatusdate
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM site_data s 
JOIN included_studies st ON (s.studyid = st.studyid);