/*
CCDM TV mapping
Notes: Standard mapping to CCDM TV table
*/

WITH included_studies AS (
	SELECT studyid FROM study
),

tv_scheduled AS (

SELECT 'KAR-004' as studyid, '99999' as visitnum, 'Visit 1' as visit, 99 as visitdy , 0 as  "visitwindowbefore", 0 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'2' as visitnum, 'Visit 2' as visit, 0 as visitdy , 0 as  "visitwindowbefore", 0 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'3' as visitnum, 'Visit 3' as visit, 2 as visitdy , 1 as  "visitwindowbefore", 1 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'4' as visitnum, 'Visit 4' as visit, 6 as visitdy , 1 as  "visitwindowbefore", 1 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'5' as visitnum, 'Visit 5' as visit, 7 as visitdy , 2 as  "visitwindowbefore", 2 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'6' as visitnum, 'Visit 6' as visit, 13 as visitdy , 2 as  "visitwindowbefore", 2 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'7' as visitnum, 'Visit 7' as visit, 20 as visitdy , 2 as  "visitwindowbefore", 2 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'8' as visitnum, 'Visit 8' as visit, 27 as visitdy , 2 as  "visitwindowbefore", 2 as "visitwindowafter"
union all
select 'KAR-004' as studyid,'9' as visitnum, 'Visit 9' as visit, 34 as visitdy , 2 as  "visitwindowbefore", 0 as "visitwindowafter"
),

tv_data AS (
	SELECT
		'KAR-004'::text AS studyid,
		coalesce(visitnum,'99')::numeric AS visitnum,
		visit::text AS visit,
		visitdy::int AS visitdy,
		visitwindowbefore::int AS visitwindowbefore,
		visitwindowafter::int AS visitwindowafter
	FROM tv_scheduled tvs

	UNION ALL

	SELECT
		DISTINCT sv.studyid::text AS studyid,
		coalesce(sv."visitnum", 99)::numeric AS visitnum,
		sv."visit"::text AS visit,
		99999::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM sv 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM tv_scheduled)

	UNION ALL
	SELECT 
		DISTINCT studyid::text AS studyid,
		'99'::numeric AS visitnum,
		visit::text AS visit,
		'99999'::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM formdata 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM sv) 
	AND (studyid, visit) NOT IN (SELECT studyid, visit FROM tv_scheduled)
	
	UNION ALL

	SELECT 
		DISTINCT studyid::text AS studyid,
		'99'::int AS visitnum,
		visit::text AS visit,
		'99999'::int AS visitdy,
		0::int AS visitwindowbefore,
		0::int AS visitwindowafter
	FROM dv 
	WHERE (studyid, visit) NOT IN (SELECT DISTINCT studyid, visit FROM sv) 
	AND (studyid, visit) NOT IN (SELECT studyid, visit FROM tv_scheduled)
  
	
)

SELECT
	/*KEY tv.studyid::text AS comprehendid, KEY*/
	tv.studyid::text AS studyid,
	tv.visitnum::numeric AS visitnum,
	tv.visit::text AS visit,
	tv.visitdy::int AS visitdy,
	tv.visitwindowbefore::int AS visitwindowbefore,
	tv.visitwindowafter::int AS visitwindowafter
	/*KEY , (tv.studyid || '~' || tv.visit)::text  AS objectuniquekey KEY*/
	/*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM tv_data tv
JOIN included_studies st ON (st.studyid = tv.studyid);

