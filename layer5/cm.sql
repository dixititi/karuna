/*
CCDM CM mapping
Notes: Standard mapping to CCDM CM table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid From subject ),

     cm_data AS (                 
                SELECT  "STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        --"RECORDPOSITION"::integer AS cmseq,
                        "CMTRT"::text AS cmtrt,
                        "CMDECOD"::text AS cmmodify,
                        "CMDECOD"::text AS cmdecod,
                        'Concomitant Medications'::text AS cmcat,
                        'Concomitant Medications'::text AS cmscat,
                        "CMINDC"::text AS cmindc,
                        "CMDOSE"::numeric AS cmdose,
                        "CMDOSU"::text AS cmdosu,
                        Null::text AS cmdosfrm,
                        "CMDOSFRQ"::text AS cmdosfrq,
                        Null::numeric AS cmdostot,
                        "CMROUTE"::text AS cmroute,
                        /*case when cmstdtc='' then null
							else to_date(cmstdtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmstdtc,
						case when cmendtc='' then null
							else to_date(cmendtc,'DD Mon YYYY') 
						end ::timestamp without time zone AS cmendtc,*/
                        null::time without time zone AS cmsttm,
                        null::time without time zone AS cmentm
                FROM  kar004_sdtm."CM"
/*( select *,concat(replace(substring(upper("CMSTDAT_RAW"),1,2),'UN','01'),replace(substring(upper("CMSTDAT_RAW"),3),'UNK','Jan')) AS cmstdtc,
	     concat(replace(substring(upper("CMENDAT_RAW"),1,2),'UN','01'),replace(substring(upper("CMENDAT_RAW"),3),'UNK','Jan')) AS cmendtc 
from KAR-004."CM"	
)cm  */
     )

SELECT 
        /*KEY (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid)::text AS comprehendid, KEY*/
        cm.studyid::text AS studyid,
        cm.siteid::text AS siteid,
        cm.usubjid::text AS usubjid,
        --cm.cmseq::integer AS cmseq,
        cm.cmtrt::text AS cmtrt,
        cm.cmmodify::text AS cmmodify,
        cm.cmdecod::text AS cmdecod,
        cm.cmcat::text AS cmcat,
        cm.cmscat::text AS cmscat,
        cm.cmindc::text AS cmindc,
        cm.cmdose::numeric AS cmdose,
        cm.cmdosu::text AS cmdosu,
        cm.cmdosfrm::text AS cmdosfrm,
        cm.cmdosfrq::text AS cmdosfrq,
        cm.cmdostot::numeric AS cmdostot,
        cm.cmroute::text AS cmroute,
        --cm.cmstdtc::timestamp without time zone AS cmstdtc,
        --cm.cmendtc::timestamp without time zone AS cmendtc,
        cm.cmsttm::time without time zone AS cmsttm,
        cm.cmentm::time without time zone AS cmentm
        /*KEY , (cm.studyid || '~' || cm.siteid || '~' || cm.usubjid || '~' || cm.cmseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM cm_data cm
JOIN included_subjects s ON (cm.studyid = s.studyid AND cm.siteid = s.siteid AND cm.usubjid = s.usubjid);