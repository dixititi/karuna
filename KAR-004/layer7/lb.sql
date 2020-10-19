WITH included_subjects AS (
                 SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     lb_data AS (
                  SELECT 
studyid,
siteid,
usubjid,
visit,
lbdtc,
lbdy,
(row_number() over (partition by lb.studyid, lb.siteid, lb.usubjid order by lb.lbtestcd, lb.lbdtc))::int as lbseq,
lbtestcd,
lbtest,
lbcat,
lbscat,
lbspec,
lbmethod,
lborres,
lbstat,
lbreasnd,
lbstnrlo,
lbstnrhi,
lborresu,
lbstresn,
lbstresu,
lbblfl,
lbnrind,
lbornrhi,
lbornrlo,
lbstresc,
lbenint,
lbevlint,
lblat,
lblloq,
lbloc,
lbpos,
lbstint,
lbuloq,
lbclsig,
lbtm from (SELECT "STUDYID"::text AS studyid,
            reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
            "USUBJID"::text AS usubjid,
            "VISIT"::text AS visit,
             to_date(substring("LBDTC",1,10),'yyyy-mm-dd')::date AS lbdtc,
            "LBDY"::integer AS lbdy,
            NULL::int AS lbseq,
            "LBTESTCD" AS lbtestcd,
            "LBTEST"::text AS lbtest,
            "LBCAT"::text AS lbcat,
            null::text AS lbscat,
            "LBSPEC"::text AS lbspec,
            "LBMETHOD"::text AS lbmethod,
            "LBORRES"::text AS lborres,
            "LBSTAT"::text AS lbstat,
            "LBREASND"::text AS lbreasnd,
            "LBSTNRLO"::numeric AS lbstnrlo,
            "LBSTNRHI"::numeric  AS lbstnrhi,
            "LBORRESU"::text AS lborresu,
            "LBSTRESN"::numeric AS  lbstresn,
            "LBSTRESU"::text AS lbstresu,
			"LBBLFL"::text AS lbblfl,
			"LBNRIND"::text AS lbnrind,
			"LBORNRHI"::text AS lbornrhi,
			"LBORNRLO"::text AS lbornrlo,
			"LBSTRESC"::text AS lbstresc,
			NULL::text AS lbenint,
			NULL::text AS lbevlint,
			NULL::text AS lblat,
			NULL::text AS lblloq,
			NULL::text AS lbloc,
			NULL::text AS lbpos,
			NULL::text AS lbstint,
			NULL::text AS lbuloq,
			NULL::text AS lbclsig,
            case when length("LBDTC")=16 then 
		    concat(substring("LBDTC",12,5),':00')::text
			when length("LBDTC")=10 then '00:00:00'::text end AS lbtm
            from kar004_sdtm."LB"
			
			UNION ALL
			
			SELECT  "STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        "VISIT"::text AS visit,
                        to_date(substring("QSDTC",1,10),'yyyy-mm-dd')::date as lbdtc,
                        null::integer AS lbdy,
                        --"QSSEQ"::int AS lbseq,
                        NULL::int AS lbseq,
                        "QSTESTCD"::text AS lbtestcd,
                        "QSTEST"::text AS lbtest,
                        'QUESTIONNAIRES'::text AS lbcat,
                        "QSCAT"::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        "QSORRES"::text AS lborres,
                        null::text AS lbstat,
                        null::text AS lbreasnd,
                        null::numeric AS lbstnrlo,
                        null::numeric AS lbstnrhi,
                        NULL::text AS lborresu,--doubt
                        "QSSTRESN"::numeric AS  lbstresn,
                        "QSSTRESC"::text AS  lbstresu, --doubt
						"QSBLFL"::text As lbblfl,
						NULL::text AS lbnrind,
						NULL::text AS lbornrhi,
						NULL::text AS lbornrlo,
						NULL::text AS lbstresc,
						NULL::text AS lbenint,
						NULL::text AS lbevlint,
						NULL::text AS lblat,
						NULL::text AS lblloq,
						NULL::text AS lbloc,
						null::text AS lbpos,
						NULL::text AS lbstint,
						NULL::text AS lbuloq,
						NULL::text AS lbclsig,
                        null::text AS lbtm
                FROM kar004_sdtm."QS"
			    -- vs mapping
                UNION ALL
                SELECT  "STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        "VISIT"::text AS visit,
                        to_date(substring("VSDTC",1,10),'yyyy-mm-dd')::date as lbdtc,
                        null::integer AS lbdy,
                        --"VSSEQ"::int AS lbseq,
                        NULL::int AS lbseq,
                        "VSTESTCD"::text AS lbtestcd,
                        "VSTEST"::text AS lbtest,
                        null::text AS lbcat,--doubt
                        'Vital Signs'::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        "VSORRES"::text AS lborres,
                        "VSSTAT"::text AS lbstat,
                        null::text AS lbreasnd,
                        null::numeric AS lbstnrlo,
                        null::numeric AS lbstnrhi,
                        "VSORRESU"::text AS lborresu,
                        "VSSTRESN"::numeric AS  lbstresn,
                        "VSSTRESU"::text AS  lbstresu,
						"VSBLFL"::text AS lbblfl,
						NULL::text AS lbnrind,
						NULL::text AS lbornrhi,
						NULL::text AS lbornrlo,
						NULL::text AS lbstresc,
						NULL::text AS lbenint,
						NULL::text AS lbevlint,
						NULL::text AS lblat,
						NULL::text AS lblloq,
						NULL::text AS lbloc,
						"VSPOS"::text AS lbpos,
						NULL::text AS lbstint,
						NULL::text AS lbuloq,
						NULL::text AS lbclsig,
                        null::text AS lbtm --doubt
                FROM kar004_sdtm."VS"
				-- EX Data
                UNION ALL
                SELECT  "STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        "DOMAIN"::text AS visit,
                        to_date(substring("EXSTDTC",1,10),'yyyy-mm-dd')::date as lbdtc,
                    null::integer as lbdy,
                    --"EXSEQ"::int AS lbseq,
                    NULL::int AS lbseq,
                   'EXPOSURE'::text AS lbtestcd,
                   'EXPOSURE'::text AS lbtest,
                   'EXPOSURE'::text AS lbcat,
                    null::text AS lbscat, --doubt
                    null::text AS lbspec,
                    null::text AS lbmethod,
                    "EXDOSE"::text AS lborres,
                    null::text AS lbstat,
                    null::text AS lbreasnd,
                    null::numeric AS lbstnrlo,
                    null::numeric AS lbstnrhi,
                    "EXDOSU"::text AS lborresu,
                    "EXDOSE"::numeric AS lbstresn,
                    "EXDOSU"::text AS lbstresu,
					null::text AS lbblfl,
					NULL::text AS lbnrind,
						NULL::text AS lbornrhi,
						NULL::text AS lbornrlo,
						NULL::text AS lbstresc,
						NULL::text AS lbenint,
						NULL::text AS lbevlint,
						NULL::text AS lblat,
						NULL::text AS lblloq,
						NULL::text AS lbloc,
					null::text AS lbpos,
					NULL::text AS lbstint,
					NULL::text AS lbuloq,
					NULL::text AS lbclsig,
                    null::text as lbtm
                FROM kar004_sdtm."EX"
				 -- EG Data
                UNION ALL
                select "STUDYID"::text AS studyid,
                        reverse(SUBSTRING(reverse("USUBJID"),5,3))::text AS siteid,
                        "USUBJID"::text AS usubjid,
                        "VISIT"::text AS visit,
                        to_date(substring("EGDTC",1,10),'yyyy-mm-dd')::date AS lbdtc,
                    null::integer as lbdy,
                    --"EGSEQ"::int AS lbseq,
                    NULL::int AS lbseq,
                    "EGTESTCD"::text AS lbtestcd,
                    "EGTEST"::text AS lbtest,
                    'ECG'::text AS lbcat,
                    "EGCAT"::text AS lbscat,
                    null::text AS lbspec,
                    null::text AS lbmethod,
                    "EGORRES"::text AS lborres,
                    "EGSTAT"::text AS lbstat,
                    null::text as lbreasnd,
                    null::NUMERIC as lbstnrlo,
                    null::numeric as lbstnrhi,
                    "EGORRESU"::text AS lborresu,
                    "EGSTRESN"::numeric AS lbstresn,
                    "EGSTRESU"::text AS lbstresu,
					"EGBLFL" ::text As lbblfl,
					NULL::text AS lbnrind,
						NULL::text AS lbornrhi,
						NULL::text AS lbornrlo,
						NULL::text AS lbstresc,
						NULL::text AS lbenint,
						NULL::text AS lbevlint,
						NULL::text AS lblat,
						NULL::text AS lblloq,
						NULL::text AS lbloc,
					null::text AS lbpos, --doubt
					NULL::text AS lbstint,
					NULL::text AS lbuloq,
					NULL::text AS lbclsig,
                    null::text AS lbtm
                from kar004_sdtm."EG" ) lb
			
         )
   
SELECT
        /*KEY (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid)::text AS comprehendid, KEY*/
        lb.studyid::text AS studyid,
        lb.siteid::text AS siteid,
        lb.usubjid::text AS usubjid,
        lb.visit::text AS visit,
        lb.lbdtc::date AS lbdtc,
        lb.lbdy::integer AS lbdy,
        lb.lbseq::integer AS lbseq,
        lb.lbtestcd::text AS lbtestcd,
        lb.lbtest::text AS lbtest,
        lb.lbcat::text AS lbcat,
        lb.lbscat::text AS lbscat,
        lb.lbspec::text AS lbspec,
        lb.lbmethod::text AS lbmethod,
        lb.lborres::text AS lborres,
        lb.lbstat::text AS lbstat,
        lb.lbreasnd::text AS lbreasnd,
        lb.lbstnrlo::numeric AS lbstnrlo,
        lb.lbstnrhi::numeric AS lbstnrhi,
        lb.lborresu::text AS lborresu,
        lb.lbstresn::numeric AS  lbstresn,
        lb.lbstresu::text AS  lbstresu,
        --lb.lbtm::time without time zone AS lbtm,
        lb.lbblfl::text AS  lbblfl,
        lb.lbnrind::text AS  lbnrind,
        lb.lbornrhi::text AS  lbornrhi,
        lb.lbornrlo::text AS  lbornrlo,
        lb.lbstresc::text AS  lbstresc,
        lb.lbenint::text AS  lbenint,
        lb.lbevlint::text AS  lbevlint,
        lb.lblat::text AS  lblat,
        lb.lblloq::numeric AS  lblloq,
        lb.lbloc::text AS  lbloc,
        lb.lbpos::text AS  lbpos,
        lb.lbstint::text AS  lbstint,
        lb.lbuloq::numeric AS  lbuloq,
        lb.lbclsig::text AS  lbclsig
        /*KEY , (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid || '~' || lb.lbseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp with time zone AS comprehend_update_time KEY*/
FROM lb_data lb
JOIN included_subjects s ON (lb.studyid = s.studyid AND lb.siteid = s.siteid AND lb.usubjid = s.usubjid);