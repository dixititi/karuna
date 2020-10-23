WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     ex_data AS (
SELECT
     studyid,
     siteid,
     usubjid,
     row_number() over (partition by ex."studyid", ex."siteid", ex."usubjid" ORDER BY ex."exstdtc")::int AS exseq,
     visit,
     extrt,
     excat,
     exscat,
     exdose,
     exdostxt,
     exdosu,
     exdosfrm,
     exdosfrq,
     exdostot,
     exstdtc,
     exsttm,
     exstdy,
     exendtc,
     exendtm,
     exendy,
     exdur
     from
(SELECT 
		reverse(substring(reverse(e."USUBJID"),5,3))::text	AS	siteid,
		e."USUBJID"::text	AS	usubjid,
		null::INT   AS exseq,
		e."STUDYID"::text	AS	studyid,
		s."VISIT"::text	AS	visit,
		'EXPOSURE'::text	AS	extrt,
		'EXPOSURE'::text	AS	excat,
		NULL::text	AS	exscat,
		"EXDOSE"::numeric	AS	exdose,
		NULL::text	AS	exdostxt,
		"EXDOSU"::text	AS	exdosu,
		"EXDOSFRM"::text	AS	exdosfrm,
		"EXDOSFRQ"::text	AS	exdosfrq,
		NULL::text	AS	exdostot,
		TO_DATE(substring("EXSTDTC",1,10),'YYYY-MM-DD')::date	AS	exstdtc,
		case when length(replace("EXSTDTC",'T',' '))=10 then concat("EXSTDTC",' 00:00:00')
			when length(replace("EXSTDTC",'T',' '))=16 then concat(replace("EXSTDTC",'T',' '),':00')
			end::text AS	exsttm,
		"EXSTDY"::text	AS	exstdy,
		TO_DATE(substring("EXENDTC",1,10),'YYYY-MM-DD')::date	AS	exendtc,
		case when length(replace("EXENDTC",'T',' '))=10 then concat("EXENDTC",' 00:00:00')
			when length(replace("EXENDTC",'T',' '))=16 then concat(replace("EXENDTC",'T',' '),':00')
			end::text	AS	exendtm,
		"EXENDY"::text	AS	exendy,
		NULL::text	AS	exdur
from kar004_sdtm."EX" e
inner join kar004_sdtm."SV" s
on e."STUDYID"=s."STUDYID" and e."USUBJID"=s."USUBJID" and e."EXSTDTC"= s."SVSTDTC" ) ex
)

SELECT
        /*KEY (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid)::text AS comprehendid, KEY*/
        ex.studyid::text AS studyid,
        ex.siteid::text AS siteid,
        ex.usubjid::text AS usubjid,
        ex.exseq::int AS exseq, 
        ex.visit::text AS visit,
        ex.extrt::text AS extrt,
        ex.excat::text AS excat,
        ex.exscat::text AS exscat,
        ex.exdose::numeric AS exdose,
        ex.exdostxt::text AS exdostxt,
        ex.exdosu::text AS exdosu,
        ex.exdosfrm::text AS exdosfrm,
        ex.exdosfrq::text AS exdosfrq,
        ex.exdostot::numeric AS exdostot,
        ex.exstdtc::date AS exstdtc,
        ex.exsttm::time AS exsttm,
        ex.exstdy::int AS exstdy,
        ex.exendtc::date AS exendtc,
        ex.exendtm::time AS exendtm,
        ex.exendy::int AS exendy,
        ex.exdur::text AS exdur
        /*KEY , (ex.studyid || '~' || ex.siteid || '~' || ex.usubjid || '~' || ex.exseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM ex_data ex
JOIN included_subjects s ON (ex.studyid = s.studyid AND ex.siteid = s.siteid AND ex.usubjid = s.usubjid);

