/*
CCDM DS mapping
Notes: Standard mapping to CCDM DS table
*/

WITH included_subjects AS (
                SELECT DISTINCT studyid, siteid, usubjid from subject ),

ds_data AS (


--Disposition Event: Consented

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
2.0::NUMERIC AS dsseq, 
'Consent'::text AS dscat,
'Consented'::text AS dsterm,      
max(ds."DSSTDTC")::DATE AS dsstdtc,
ds."DSDECOD"::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" = 'INFORMED CONSENT OBTAINED'
group by 1,2,3,8

union all 

--Disposition Event: Not Consented

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
1.1::NUMERIC AS dsseq, 
'Consent'::text AS dscat,
'No Consent'::text AS dsterm,   
ds."DSSTDTC"::DATE AS dsstdtc,
ds."DSDECOD"::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" = 'CONSENT WITHDRAWN'

union all 

--Disposition Event: Failed Screen

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
2.1::NUMERIC AS dsseq, 
'Enrollment'::text AS dscat,
'Failed Screen'::text AS dsterm,   
ds."DSSTDTC"::DATE AS dsstdtc,
ds."DSDECOD"::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" = 'SCREEN FAILURE'
  
union all 

--Disposition Event: Enrollment

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
3.0::NUMERIC AS dsseq, 
'Enrollment'::text AS dscat,
'Enrolled'::text AS dsterm,   
ds."DSSTDTC"::DATE AS dsstdtc,  
'Enrolled'::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" = 'RANDOMIZED'

union all 

--Disposition Event: Randomized

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
4.0::NUMERIC AS dsseq, 
'Randomization'::text AS dscat,
'Randomized'::text AS dsterm,   
ds."DSSTDTC"::DATE AS dsstdtc, 
'Randomized'::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" = 'RANDOMIZED'

union all 

--Disposition Event: Study Completion

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
5.0::NUMERIC AS dsseq, 
'Completion'::text AS dscat,
'Completed'::text AS dsterm,   
max(ds."DSSTDTC")::DATE AS dsstdtc,
ds."DSDECOD"::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" = 'COMPLETED'
group by 1, 2, 3, 8


union all 

--Disposition Event: Withdrawn

SELECT  ds."STUDYID"::text AS studyid,
reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
ds."USUBJID"::text AS usubjid,
4.1::NUMERIC AS dsseq, 
'Completion'::text AS dscat,
'Withdrawn'::text AS dsterm,   
max(ds."DSSTDTC")::DATE AS dsstdtc,
ds."DSDECOD"::text AS dsscat 
from kar004_sdtm."DS" ds
where "DSTERM" not in ('INFORMED CONSENT OBTAINED', 'CONSENT WITHDRAWN', 'SCREEN FAILURE', 'COMPLETED')
group by 1,2,3,8

)

SELECT
        /*KEY (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid)::text AS comprehendid, KEY*/
        ds.studyid::text AS studyid,
        ds.siteid::text AS siteid,
        ds.usubjid::text AS usubjid,
        ds.dsseq::NUMERIC AS dsseq,
        ds.dscat::text AS dscat,
        ds.dsscat::text AS dsscat,
        ds.dsterm::text AS dsterm,
        ds.dsstdtc::DATE AS dsstdtc
        /*KEY , (ds.studyid || '~' || ds.siteid || '~' || ds.usubjid || '~' || ds.dsseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::TIMESTAMP WITH TIME ZONE AS comprehend_update_time KEY*/
FROM ds_data ds
JOIN included_subjects s ON (ds.studyid = s.studyid AND ds.siteid = s.siteid AND ds.usubjid = s.usubjid);