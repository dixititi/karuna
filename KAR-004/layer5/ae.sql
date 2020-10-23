/*
CCDM AE mapping
Notes: Standard mapping to CCDM AE table
*/

WITH included_subjects AS (
    SELECT DISTINCT studyid, siteid, usubjid from subject),


    ae_data AS ( select studyid, siteid, usubjid, aeterm, aeverbatim, aebodsys, aestdtc, aeendtc, 
                        aesev, aeser, aerelnst, aesttm, aeentm, aellt, aelltcd, aeptcd, aehlt, aehltcd, 
                        aehlgt, aehlgtcd, aebdsycd, aesoc, aesoccd, aeacn,
                        ROW_NUMBER () OVER (PARTITION BY studyid, siteid, usubjid) as aeseq from
                   (SELECT "STUDYID"::text AS studyid,
                       reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                       ae."USUBJID"::text AS usubjid,
                       ae."AEDECOD"::text AS aeterm,
                       ae."AETERM"::text AS aeverbatim,
                       ae."AEBODSYS"::text AS aebodsys,
                       
                       case when length(replace("AESTDTC",'T',' '))=10 then concat("AESTDTC",' 00:00:00')
                       when length(replace("AESTDTC",'T',' '))=16 then concat(replace("AESTDTC",'T',' '),':00')
                       end::text aestdtc,
                       
		               case when length(replace("AEENDTC",'T',' '))=10 then concat("AEENDTC",' 00:00:00')
                       when length(replace("AEENDTC",'T',' '))=16 then concat(replace("AEENDTC",'T',' '),':00')
                       end::text AS	aeendtc,
                       
                       ae."AESEV"::text AS aesev,
					   case when lower(ae."AESER")='y' then 'Serious' 
                       when lower(ae."AESER")='n' then 'Non-Serious' 
                       else 'Unknown' end::text AS aeser ,			  
					   
					   CASE WHEN lower("AEREL") in (lower('LIKELY RELATED'), lower('RELATED'), lower('POSSIBLY RELATED')) THEN 'Yes' 
					   WHEN lower("AEREL") in (lower('NOT RELATED'), lower('UNLIKELY RELATED')) THEN 'No'  
					   else 'Unknown' END::text AS aerelnst,

					   null::time without time zone AS aesttm,
					   null::time without time zone AS aeentm,
                       ae."AELLT" ::text AS aellt,
					   ae."AELLTCD":: int AS aelltcd,
					   ae."AEPTCD":: int AS aeptcd,
					   ae."AEHLT" ::text AS aehlt,
					   ae."AEHLTCD":: int AS aehltcd,
					   ae."AEHLGT" ::text AS aehlgt,
					   ae."AEHLGTCD":: int AS aehlgtcd,
					   ae."AEBDSYCD":: int AS aebdsycd,
					   ae."AESOC" ::text AS aesoc,
					   ae."AESOCCD":: int AS aesoccd,
					  					   
					   CASE WHEN lower("AEACN")='other' THEN ae."AEACN"||ae."AEACNOTH" 					     
					   else ae."AEACN" END::text AS aeacn
		        FROM kar004_sdtm."AE" ae ) ae_sub
  )

SELECT
        /*KEY (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid)::text AS comprehendid, KEY*/
        ae.studyid::text AS studyid,
        ae.siteid::text AS siteid,
        ae.usubjid::text AS usubjid,
        ae.aeterm::text AS aeterm,
        ae.aeverbatim::text AS aeverbatim,
        ae.aebodsys::text AS aebodsys,
        ae.aestdtc::timestamp without time zone AS aestdtc,
        ae.aeendtc::timestamp without time zone AS aeendtc,
        ae.aesev::text AS aesev,
        ae.aeser::text AS aeser,
        ae.aerelnst::text AS aerelnst,
        ae.aeseq::int AS aeseq,
        ae.aesttm::time without time zone AS aesttm,
        ae.aeentm::time without time zone AS aeentm,
		ae.aellt::text AS aellt,
		ae.aelltcd::int AS aelltcd,
		ae.aeptcd::int AS aeptcd,
		ae.aehlt::text AS aehlt,
		ae.aehltcd::int AS aehltcd,
		ae.aehlgt::text AS aehlgt,
		ae.aehlgtcd::int AS aehlgtcd,
		ae.aebdsycd::int AS aebdsycd,
		ae.aesoc::text AS aesoc,
		ae.aesoccd::int AS aesoccd,
		ae.aeacn::text AS aeacn
		
		
        /*KEY , (ae.studyid || '~' || ae.siteid || '~' || ae.usubjid || '~' || ae.aeseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM ae_data ae
JOIN included_subjects s ON (ae.studyid = s.studyid AND ae.siteid = s.siteid AND ae.usubjid = s.usubjid);

