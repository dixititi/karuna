/*
CCDM VS mapping
Notes: Standard mapping to CCDM VS table
*/
with included_subjects as (
select
	distinct studyid,
	siteid,
	usubjid
from subject),

vs_data as (
select
	studyid,
	siteid,
	usubjid,
	(row_number() over (partition by studyid, siteid, usubjid
	order by vsdtc, vstm))::int as vsseq,
	vstestcd,
	vstest,
	vscat,
	vsscat,
	vspos,
	vsorres,
	vsorresu,
	vsstresn,
	vsstresu,
	vsstat,
	vsloc,
	vsblfl,
	visit,
	vsdtc,
	vstm
	from (
		select vs."STUDYID"::text as studyid,
		reverse(substring(reverse("USUBJID"), 5, 3))::text as siteid,
		"USUBJID"::text as usubjid,
		null::int as vsseq,
		"VSTESTCD"::text as vstestcd,
		"VSTEST"::text as vstest,
		'Vital Signs'::text as vscat,
		'Vital Signs'::text as vsscat,
		"VSPOS"::text as vspos,
		"VSORRES"::text as vsorres,
		"VSORRESU"::text as vsorresu,
		"VSSTRESN"::numeric as vsstresn,
		"VSSTRESU"::text as vsstresu,
		"VSSTAT"::text as vsstat,
		null::text as vsloc,
		"VSBLFL"::text as vsblfl,
		"VISIT"::text as visit,
		case when length(replace("VSDTC",'T',' '))=10 then concat("VSDTC",' 00:00:00')
        when length(replace("VSDTC",'T',' '))=16 then concat(replace("VSDTC",'T',' '),':00')
         end::text vsdtc,
		case when length(replace("VSDTC",'T',' '))=10 then concat("VSDTC",' 00:00:00')
        when length(replace("VSDTC",'T',' '))=16 then concat(replace("VSDTC",'T',' '),':00')
         end::text	AS	vstm		
		from kar004_sdtm."VS" vs ) vs_sub) 
		
		select
	/*KEY (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid)::text AS comprehendid, KEY*/
	vs.studyid::text as studyid,
	vs.siteid::text as siteid,
	vs.usubjid::text as usubjid,
	vs.vsseq::int as vsseq,
	vs.vstestcd::text as vstestcd,
	vs.vstest::text as vstest,
	vs.vscat::text as vscat,
	vs.vsscat::text as vsscat,
	vs.vspos::text as vspos,
	vs.vsorres::text as vsorres,
	vs.vsorresu::text as vsorresu,
	vs.vsstresn::numeric as vsstresn,
	vs.vsstresu::text as vsstresu,
	vs.vsstat::text as vsstat,
	vs.vsloc::text as vsloc,
	vs.vsblfl::text as vsblfl,
	vs.visit::text as visit,
	vs.vsdtc::timestamp without time zone AS vsdtc,
	vs.vstm::time without time zone AS vstm
 /*KEY , (vs.studyid || '~' || vs.siteid || '~' || vs.usubjid || '~' || vs.vsseq)::text  AS objectuniquekey KEY*/
	/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
from
	vs_data vs
join included_subjects s on (vs.studyid = s.studyid and vs.siteid = s.siteid and vs.usubjid = s.usubjid);

