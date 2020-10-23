WITH included_subjects AS (
                 SELECT DISTINCT studyid, siteid, usubjid FROM subject),

     lb_data2 AS (
        SELECT lb.studyid::text AS studyid,
            lb.siteid::text AS siteid,
            lb.usubjid::text AS usubjid,
            lb.visit::text AS visit,
            lb.lbdtc::timestamp without time zone AS lbdtc,
            lb.lbdy::integer AS lbdy,
            (row_number() over (partition by lb.studyid, lb.siteid, lb.usubjid order by lb.lbseq, lb.lbdtc))::int as lbseq,
            case
				 when lbtestcd ilike '%Alkaline Phosphatase%' then 'ALP'
				 when lbtestcd ilike '%Aspartate aminotransferase (AST)%' then 'AST'
				 when lbtestcd ilike '%Alanine aminotransferase (ALT)%' then 'ALT'
                 WHEN lbtestcd in ('Total Bilirubin','Bilirubin (Total)') THEN 'BILI'
				 else lbtestcd
                 end::text AS lbtestcd,
            lb.lbtest::text AS lbtest,
            upper(lb.lbcat)::text AS lbcat,
            lb.lbscat::text AS lbscat,
            lb.lbspec::text AS lbspec,
            lb.lbmethod::text AS lbmethod,
            lb.lborres::text AS lborres,
            lb.lbstat::text AS lbstat,
            lb.lbreasnd::text AS lbreasnd,
            convert_to_numeric(lb.lbstnrlo::text)::numeric AS lbstnrlo,
            convert_to_numeric(lb.lbstnrhi::text)::numeric  AS lbstnrhi,
            lb.lborresu::text AS lborresu,
            CASE WHEN lb.lborres ~ '^[0-9\.]+$' then nullif(nullif(regexp_replace(lb.lborres , '[^0-9.]*', '', 'g'), ''), '.')::numeric
                 WHEN (lb.lborres like '%/%') OR (lb.lborres like '%-%') THEN null::numeric
            END::numeric AS  lbstresn,
            lb.lbstresu::text AS lbstresu,
			lb.lbblfl::text AS lbblfl,
			lb.lbpos::text AS lbpos,
            lb.lbtm::time without time zone AS lbtm
         FROM (

               -- DCRA1AT101_COVANCE_LAB
                SELECT  "STUDYID"::text AS studyid,
                        "SITEID"::text AS siteid,
                        "SUBJID"::text AS usubjid,
                        "VISIT"::text AS visit,
                        "LBDTM"::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        null::text AS lbseq,
                        "LBTEST"::text AS lbtestcd, 
						CASE WHEN "BATTRNAM" IN ('URINE MACRO & MICRO PANEL')AND
						"LBTEST"  in ('Ur pH','Ur Specific Gravity')THEN 'URINE MACRO & MICRO PANEL'
						when "BATTRNAM" in ( 'CASTS') then 'CASTS' 
						when "BATTRNAM" in ('CELLULAR ELEMENTS')THEN 'CELLULAR ELEMENTS'
						else "LBTEST" 
						end::text as lbtest,
                       CASE
						WHEN "BATTRNAM" IN  ('CHEMISTRY PANEL','CHEMISTRY PANEL II','C-REACTIVE PROTEIN','FOLLICLE STIMULATING HORMONE','GFR BY MDRD','THYROID STIMULATING HORMONE'
						) THEN 'CHEMISTRY'
						WHEN "BATTRNAM" = 'COAGULATION GROUP' THEN 'COAGULATION'
						WHEN "BATTRNAM" LIKE 'COMPLEMENT%' THEN 'COMPLEMENT'
						WHEN "BATTRNAM" IN ('HEMATOLOGY&DIFFERENTIAL PANEL','PLATELETS','ALPHA FETOPROTEIN','RETICULOCYTE COUNT'
						) THEN 'HEMATOLOGY'
						WHEN "LBTEST" IN ('Alpha-1-Antitrypsin') THEN 'HEMATOLOGY'
						WHEN "BATTRNAM" LIKE '%CYTOKINE%' THEN 'CYTOKINE'
						WHEN "BATTRNAM" IN ('URINE MACRO & MICRO PANEL')AND
						"LBTEST"  in ('Ur pH','Ur Specific Gravity')THEN 'URINALYSIS'
						WHEN "BATTRNAM" IN ('CASTS','CELLULAR ELEMENTS')THEN 'URINALYSIS'					
						else "BATTRNAM"
						END::text AS lbcat,
                        "LBTEST"::text AS lbscat,
                        "LBSPEC"::text AS lbspec,
                        null::text AS lbmethod,
                        "RPTRESN"::text AS lborres,
                        case when "TSTSTAT" = 'X' then 'Not completed'
						when "TSTSTAT" = 'D' then 'Completed' end
						::text AS lbstat,
                        null::text AS lbreasnd,
                        "SINRLO"::text AS lbstnrlo,
                        "SINRHI"::text AS lbstnrhi,
                        "RPTU"::text AS lborresu,
                        "SIRESN"::text AS lbstresn,
                        "SIU"::text AS lbstresu,
						CASE WHEN "VISIT"= 'Baseline/Day -1' and "BATTRNAM" = 'URINE MACRO & MICRO PANEL' and "LBTEST" not in ('Ur pH','Ur Specific Gravity') THEN 1
						when "VISIT"= 'Dosing/Day 1' THEN 1 ELSE 0 END::text as lbblfl,
						null::text AS lbpos,
                        convert_to_timestamp("LBDTM"::text)::time without time zone AS lbtm
              FROM "dcr_a1at_101_covance_lab"."DCRA1AT101_COVANCE_LAB"
				 where upper("BATTRNAM") not in (
						'HEPATITIS PANEL',
						'HIV 1/2 AG/AB SCREEN',
						'MISCELLANEOUS ELEMENTS','ALT CHANGE FROM SCREEN','AST CHANGE FROM SCREEN',
						'T. BILI CHANGE FROM SCREEN','MICROORGANISMS','GLUTAMATE DEHYDROGENASE','CRYSTALS','AST CHANGE FROM BASELINE','ALT CHANGE FROM BASELINE') and upper("LBTEST") not in ('UR BILIRUBIN','DIRECT BILIRUBIN') 


                
                -- qs mapping
                UNION ALL
                SELECT  studyid::text AS studyid,
                        siteid::text AS siteid,
                        usubjid::text AS usubjid,
                        visit::text AS visit,
                        nullif("qsdtc"::text, '')::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        "qsseq"::text AS lbseq,
                        qstestcd::text AS lbtestcd,
                        qstest::text AS lbtest,
                        'QUESTIONNAIRES'::text AS lbcat,
                        qscat::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        qsorres::text AS lborres,
                        null::text AS lbstat,
                        null::text AS lbreasnd,
                        null::text AS lbstnrlo,
                        null::text AS lbstnrhi,
                        qsorresu::text AS lborresu,
                        qsstresn::text AS  lbstresn,
                        qsstresu::text AS  lbstresu,
						qsblfl::text As lbblfl,
						null::text AS lbpos,
                        null::time without time zone AS lbtm
                FROM qs
			    -- vs mapping
                UNION ALL
                SELECT  studyid::text AS studyid,
                        siteid::text AS siteid,
                        usubjid::text AS usubjid,
                        visit::text AS visit,
                        nullif("vsdtc"::text, '')::timestamp without time zone AS lbdtc,
                        null::integer AS lbdy,
                        "vsseq"::text AS lbseq,
                        vstestcd::text AS lbtestcd,
                        vstest::text AS lbtest,
                        upper(vscat)::text AS lbcat,
                        'Vital Signs'::text AS lbscat,
                        null::text AS lbspec,
                        null::text AS lbmethod,
                        vsorres::text AS lborres,
                        vsstat::text AS lbstat,
                        null::text AS lbreasnd,
                        null::text AS lbstnrlo,
                        null::text AS lbstnrhi,
                        vsorresu::text AS lborresu,
                        vsstresn::text AS  lbstresn,
                        vsstresu::text AS  lbstresu,
						vsblfl::text AS lbblfl,
						vspos::text AS lbpos,
                        vstm::time without time zone AS lbtm
                FROM vs 
				-- EX Data
                UNION ALL
                SELECT  studyid::text AS studyid,
                    siteid::text AS siteid,
                    usubjid::text AS usubjid,
                    visit::text AS visit,
                    exstdtc::timestamp without time zone lbdtc,
                    null::integer as lbdy,
                    exseq::text AS lbseq,
                   'EXPOSURE'::text AS lbtestcd,
                   'EXPOSURE'::text AS lbtest,
                   'EXPOSURE'::text AS lbcat,
                    exscat::text AS lbscat,
                    null::text AS lbspec,
                    null::text AS lbmethod,
                    exdose::text AS lborres,
                    null::text AS lbstat,
                    null::text AS lbreasnd,
                    null::text AS lbstnrlo,
                    null::text AS lbstnrhi,
                    exdosu::text AS lborresu,
                    exdose::text AS lbstresn,
                    exdosu::text AS lbstresu,
					null::text AS lbblfl,
					null::text AS lbpos,
                    null::time without time zone as lbtm
                FROM ex
				 -- EG Data
                UNION ALL
                select studyid::text AS studyid,
                    siteid::text AS siteid,
                    usubjid::text AS usubjid,
                    visit::text AS visit,
                    egdtc::timestamp without time zone AS lbdtc,
                    null::integer as lbdy,
                    egseq::text AS lbseq,
                    egtestcd::text AS lbtestcd,
                    egtest::text AS lbtest,
                    'ECG'::text AS lbcat,
                    egscat::text AS lbscat,
                    null::text AS lbspec,
                    null::text AS lbmethod,
                    egorres::text AS lborres,
                    egstat::text AS lbstat,
                    null::text as lbreasnd,
                    null::text as lbstnrlo,
                    null::text as lbstnrhi,
                    egorresu::text AS lborresu,
                    egstresn::text AS lbstresn,
                    egstresu::text AS lbstresu,
					egblfl ::text As lbblfl,
					egpos::text AS lbpos,
                    egtm::time without time zone AS lbtm
                from eg 
                ) lb  WHERE  NULLIF(lb.lbtestcd , '')IS NOT NULL  ),
                
          lb_data as (     SELECT
               lb.studyid::text AS studyid,
        lb.siteid::text AS siteid,
        lb.usubjid::text AS usubjid,
        trim(lb.visit)::text AS visit,
        convert_to_timestamp(concat(lb.lbdtc::date, ' ' , lb.lbtm::time ))::timestamp without time zone AS lbdtc,
        lb.lbdy::integer AS lbdy,
        lb.lbseq::integer AS lbseq,
        trim(lb.lbtestcd)::text AS lbtestcd,
        trim(lb.lbtest)::text AS lbtest,
        lb.lbcat::text AS lbcat,
        lb.lbscat::text AS lbscat,
        lb.lbspec::text AS lbspec,
        lb.lbmethod::text AS lbmethod,
        lb.lborres::text AS lborres,
        lb.lbstat::text AS lbstat,
        lb.lbreasnd::text AS lbreasnd,
        lb.lbstnrlo::numeric AS lbstnrlo,
        lb.lbstnrhi::numeric  AS lbstnrhi,
        lb.lborresu::text AS lborresu,
        lb.lbstresn::numeric AS  lbstresn,
        lb.lbstresu::text AS lbstresu,
		lb.lbblfl::text AS lbblfl,
		lb.lbpos::text AS lbpos,
        lb.lbtm::time without time zone AS lbtm
    FROM lb_data2 lb
   where lb.lbcat  not in ('URINE MACRO & MICRO PANEL'))
   
  
   
				
	SELECT
        /*KEY (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid)::text AS comprehendid, KEY*/
        lb.studyid::text AS studyid,
        lb.siteid::text AS siteid,
        lb.usubjid::text AS usubjid,
        trim(lb.visit)::text AS visit,
        convert_to_timestamp(concat(lb.lbdtc::date, ' ' , lb.lbtm::time ))::timestamp without time zone AS lbdtc,
        lb.lbdy::integer AS lbdy,
        lb.lbseq::integer AS lbseq,
        trim(lb.lbtestcd)::text AS lbtestcd,
        trim(lb.lbtest)::text AS lbtest,
        lb.lbcat::text AS lbcat,
        lb.lbscat::text AS lbscat,
        lb.lbspec::text AS lbspec,
        lb.lbmethod::text AS lbmethod,
        lb.lborres::text AS lborres,
        lb.lbstat::text AS lbstat,
        lb.lbreasnd::text AS lbreasnd,
        lb.lbstnrlo::numeric AS lbstnrlo,
        lb.lbstnrhi::numeric  AS lbstnrhi,
        lb.lborresu::text AS lborresu,
        lb.lbstresn::numeric AS  lbstresn,
        lb.lbstresu::text AS lbstresu,
		lb.lbblfl::text AS lbblfl,
		lb.lbpos::text AS lbpos,
        lb.lbtm::time without time zone AS lbtm
        /*KEY , (lb.studyid || '~' || lb.siteid || '~' || lb.usubjid || '~' || lb.lbseq)::text  AS objectuniquekey KEY*/
        /*KEY , now()::timestamp without time zone AS comprehend_update_time KEY*/
FROM lb_data lb
JOIN included_subjects s ON (lb.studyid = s.studyid AND lb.siteid = s.siteid AND lb.usubjid = s.usubjid);
