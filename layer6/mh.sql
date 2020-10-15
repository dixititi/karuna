/*
CCDM MH mapping
Notes: Standard mapping to CCDM MH table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     mh_data AS (
				select studyid,siteid,usubjid,mhterm,mhdecod,mhcat,mhscat,mhbodsys,mhstdtc,mhsttm,mhendtc,mhendtm,
     (row_number() over (partition by studyid,siteid,usubjid order by mhstdtc,mhsttm))::int AS mhseq
     from
     					(SELECT  "STUDYID"::text AS studyid,
                        reverse(substring(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        "MHTERM"::text AS mhterm,
                        "MHDECOD"::text AS mhdecod,
                        'Medical History'::text AS mhcat,
                        'Medical History'::text AS mhscat,
                        "MHBODSYS"::text AS mhbodsys,
                        case when length("MHSTDTC")=10 then "MHSTDTC"
	 						 when length("MHSTDTC")=4 then concat("MHSTDTC",'-01-01')
	 						when length("MHSTDTC")=7 then concat("MHSTDTC",'-01')
	 						end::date AS mhstdtc,
                        null::time without time zone AS mhsttm,
                        case when length("MHENDTC")=10 then "MHENDTC"
	 when length("MHENDTC")=4 then concat("MHENDTC",'-01-01')
	 when length("MHENDTC")=7 then concat("MHENDTC",'-01')
	 end::date AS mhendtc,
                        null::time without time zone AS mhendtm 
                FROM kar004_sdtm."MH" ) z
     )

SELECT
        /*KEY (mh.studyid || '~' || mh.siteid || '~' || mh.usubjid)::text AS comprehendid, KEY*/
        mh.studyid::text AS studyid,
        mh.siteid::text AS siteid,
        mh.usubjid::text AS usubjid,
        mh.mhseq::int AS mhseq,
        mh.mhterm::text AS mhterm,
        mh.mhdecod::text AS mhdecod,
        mh.mhcat::text AS mhcat,
        mh.mhscat::text AS mhscat,
        mh.mhbodsys::text AS mhbodsys,
        mh.mhstdtc::date AS mhstdtc,
        mh.mhsttm::time without time zone AS mhsttm,
        mh.mhendtc::date AS mhendtc,
        mh.mhendtm::time without time zone AS mhendtm
        /*KEY , (mh.studyid || '~' || mh.siteid || '~' || mh.usubjid || '~' || mh.mhseq)::text AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM mh_data mh
JOIN included_subjects s ON (mh.studyid = s.studyid AND mh.siteid = s.siteid AND mh.usubjid = s.usubjid);