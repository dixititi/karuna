/*
CCDM QS mapping
Notes: Standard mapping to CCDM QS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     qs_data AS (
                select studyid, siteid, usubjid, qstestcd, qstest, qscat, qsscat, qsorres, qsorresu, 
                 qsstresc, qsstresn, qsstresu, qsstat, qsblfl, visit, qsdtc, qstm,
                 row_number() over (partition by studyid, siteid, usubjid order by qsdtc,qstm)::int AS qsseq
              FROM ( SELECT  "STUDYID"::text AS studyid,
                        reverse(substring(reverse("USUBJID"), 5, 3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        null::int AS qsseq, /*(row_number() over (partition by [studyid],[siteid],[usubjid] order [qsdtc,qstm]))::int AS qsseq,*/
                        "QSTESTCD"::text AS qstestcd,
                        "QSTEST"::text AS qstest,
                        "QSCAT"::text AS qscat,
                        "QSSCAT"::text AS qsscat,
                        "QSORRES"::text AS qsorres,
                        'NA'::text AS qsorresu,
                        "QSSTRESC"::text AS qsstresc,
                        "QSSTRESN"::numeric AS qsstresn,
                        'NA'::text AS qsstresu,
                        "QSSTAT"::text AS qsstat,
                        "QSBLFL"::text AS qsblfl,
                        "VISIT"::text AS visit,
                        
                        case when length(replace("QSDTC",'T',' '))=10 then concat("QSDTC",' 00:00:00')
                        when length(replace("QSDTC",'T',' '))=16 then concat(replace("QSDTC",'T',' '),':00')
                        end::text qsdtc,
                        
		                case when length(replace("QSDTC",'T',' '))=10 then concat("QSDTC",' 00:00:00')
                        when length(replace("QSDTC",'T',' '))=16 then concat(replace("QSDTC",'T',' '),':00')
                        end::text AS qstm	
                       from kar004_sdtm."QS" qs ) qs_sub)

SELECT
        /*KEY (qs.studyid || '~' || qs.siteid || '~' || qs.usubjid)::text AS comprehendid, KEY*/
        qs.studyid::text AS studyid,
        qs.siteid::text AS siteid,
        qs.usubjid::text AS usubjid,
        qs.qsseq::int AS qsseq,
        qs.qstestcd::text AS qstestcd,
        qs.qstest::text AS qstest,
        qs.qscat::text AS qscat,
        qs.qsscat::text AS qsscat,
        qs.qsorres::text AS qsorres,
        qs.qsorresu::text AS qsorresu,
        qs.qsstresc::text AS qsstresc,
        qs.qsstresn::numeric AS qsstresn,
        qs.qsstresu::text AS qsstresu,
        qs.qsstat::text AS qsstat,
        qs.qsblfl::text AS qsblfl,
        qs.visit::text AS visit,        
        qs.qsdtc::timestamp without time zone AS qsdtc,
        qs.qstm::time without time zone AS qstm
        --qs.qstm::timestamp without time zone AS qstm
        /*KEY , (qs.studyid || '~' || qs.siteid || '~' || qs.usubjid || '~' || qs.qsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM qs_data qs
JOIN included_subjects s ON (qs.studyid = s.studyid AND qs.siteid = s.siteid AND qs.usubjid = s.usubjid);

