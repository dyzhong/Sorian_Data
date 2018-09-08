CREATE OR REPLACE PROCEDURE KMSF.KMSFUKFINDATADUMPSP_MASTER2()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
 declare ReProcOptV INTEGER; FsclYrPerV VARCHAR(6) ; FsclStartDate VARCHAR(8) ; FsclStopDate VARCHAR(8) ; VMFsclStartDate VARCHAR(8) ; VMFsclStopDate VARCHAR(8) ; ExportPath VARCHAR(255) ; ExportName VARCHAR(255) ; StartDtimeV DATETIME ; EndDtimeV DATETIME ; RsltCdV BYTEINT ; RsltTxtV varchar(255) ; FsclYrPerRd RECORD; REC RECORD; LOGSEQ BIGINT; MAXACCTFCLYER RECORD; ACCTYERV VARCHAR(6); DATERANGEYEARPER RECORD; FLAGV INTEGER; STATEMENT VARCHAR(5000); SUBSTART TIMESTAMP; BEGIN StartDtimeV := NOW(); ReProcOptV := NVL(ReProcOptV,0); FLAGV :=0; DROP TABLE KMSF.FSCLYRPERRDT IF EXISTS; CREATE TABLE KMSF.FSCLYRPERRDT (FSCLYRPERMIN) AS (SELECT MIN(FsclYrPer) From KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD WHERE FSCLYRPER > NVL((SELECT MAX(FSCLYRPER) FROM KMSF.KMSFUKFINDATADUMPPROCLOG WHERE NVL(RSLTCD,0) = 1),'201605') AND date(CAST(FSCLPEREND AS DATE))+5 <= DATE(now())); FOR REC IN SELECT * FROM KMSF.FSCLYRPERRDT LOOP; IF REC.FSCLYRPERMIN IS NULL THEN RAISE NOTICE 'NO FSCLYRPERMIN FOUND'; RETURN 1; ELSE FsclYrPerV = REC.FSCLYRPERMIN; RAISE NOTICE '%', REC.FSCLYRPERMIN; END IF; END LOOP; DELETE FROM KMSF.KMSFUKFINDATADUMPPROCLOGDTL WHERE LOGENTRYID IN (SELECT LOGENTRYID FROM KMSF.KMSFUKFINDATADUMPPROCLOG WHERE FSCLYRPER = FsclYrPerV); DELETE FROM KMSF.KMSFUKFINDATADUMPPROCLOG WHERE FSCLYRPER = FsclYrPerV; FOR MAXACCTFCLYER IN SELECT NEXT VALUE FOR KMSF.KMSFUKFINDATADUMPPROCLOG_SEQ AS SEQN LOOP; LOGSEQ = MAXACCTFCLYER.SEQN; END LOOP; IF NVL(FsclYrPerV, '') = '' THEN FLAGV :=1; RSLTTXTV = 'Process Failed - No Fiscal Period Was Passed'; ELSIF FsclYrPerV <'201512' THEN FLAGV :=1; RsltTxtV = 'Process Failed - The Minimum Fiscal Year Period Was Violated'; ELSE FOR MAXACCTFCLYER IN SELECT MAX(FsclYrPer) AS MAXDATE FROM KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD WHERE EXP_END_DATE='9999-12-31' AND date(CAST(FSCLPEREND AS DATE))+5 <= DATE(NOW()) LOOP; IF FOUND THEN ACCTYERV = MAXACCTFCLYER.MAXDATE; ELSE FLAGV :=1; RsltTxtV = 'Process Failed - The Maximum Fiscal Year Period Was Violated'; END IF; END LOOP; END IF; IF FLAGV = 1 THEN INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOG (LOGENTRYID, REPROCOPT, FSCLYRPER, STARTDTIME, ENDDTIME, RSLTCD, RSLTTXT) VALUES (LOGSEQ, ReProcOptV, FsclYrPerV, DATE(StartDtimeV), NOW(), 0, RsltTxtV ) ; RETURN 1; END IF ; FOR DATERANGEYEARPER IN SELECT CAST(REPLACE(FSCLPERSTART,'-','') AS VARCHAR(10)) AS FSCLPERSTART, CAST(REPLACE(FSCLPEREND,'-','') AS VARCHAR(10)) AS FSCLPEREND, CASE WHEN ADD_MONTHS(CAST(FSCLPERSTART AS DATE), -5) <'2015-12-01' THEN '20151201' ELSE CAST(REPLACE(ADD_MONTHS(CAST(FSCLPERSTART AS DATE), -5),'-','') AS VARCHAR(10)) END AS VMFSTART FROM KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD WHERE EXP_END_DATE ='9999-12-31' AND FSCLYRPER=FsclYrPerV LOOP; IF FOUND THEN FsclStartDate = DATERANGEYEARPER.FSCLPERSTART ; FsclStopDate =DATERANGEYEARPER.FSCLPEREND ; VMFsclStartDate = DATERANGEYEARPER.VMFSTART ; VMFsclStopDate =DATERANGEYEARPER.FSCLPEREND ; END IF; END LOOP; RSLTCDV :=0; ExportPath := '/export/home/etluser/UK_FINANCIAL_EXTRACT/' ; SUBSTART := NOW(); ExportName := 'KMSF.UKFinBusUnitMstr'; DROP TABLE KMSF.UKFinBusUnitMstr IF EXISTS; STATEMENT := 'CREATE  TABLE ' || ExportName || ' AS ( SELECT  HPOObjId AS BusUnitHpoObjId ,ShortName AS BusUnitShortName  FROM KMSF.DSS__SMSDSS__HPOMSTRV WHERE TypeMne = ''Bus Unit'')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl'); SUBSTART := NOW(); DROP TABLE KMSF.UKFinDivMstr IF EXISTS; ExportName :='KMSF.UKFinDivMstr'; STATEMENT := 'CREATE  TABLE '|| ExportName || ' AS ( SELECT  HPOObjId AS DivHpoObjId ,ShortName AS DivShortName  FROM KMSF.DSS__SMSDSS__HPOMSTRV WHERE TypeMne = ''Division'')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl'); SUBSTART := NOW(); DROP TABLE KMSF.UKFinHlthProfMstr IF EXISTS; ExportName :='KMSF.UKFinHlthProfMstr'; STATEMENT = 'CREATE  TABLE ' || ExportName || ' AS
				(SELECT  hp.HlthProfObjId AS HlthProfObjId 
				  ,hp.HlthProfNPI AS HlthProfNPI 
				  ,hp.DegreeText   AS DegreeText 
				  ,hp.GivenName		AS FirstName 
				  ,hp.MiddleName   AS MiddleName 
				  ,hp.FamName		AS LastName 
				  ,NPIDTL.HLTHPROFPRITXNMYCD AS SPCLTYCDVAL
				  ,SPCLTY.MNE AS SPCLTYCDMNE
				  ,SPCLTY.DescText as SpcltyCdDesc 
				  ,HP.TYPEMNE AS HLTHPROFTYPEMNE
				FROM KMSF.DSS__SMSDSS__HLTHPROFMSTRV hp
				JOIN KMSF.DSS__CUSTOMER__KMSFHLTHPROFNPIDTL NPIDTL ON HP.HLTHPROFNPI=NPIDTL.HLTHPROFNPI AND EXP_END_DATE=''9999-12-31''
				LEFT JOIN KMSF.DSS__SMSDSS__CDVALUES_SPECIALTY AS spclty ON spclty.CrossRefCd = NPIDtl.HlthProfPriTxnmyCd)'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_030_LoadHlthProfMstrTbl'); SUBSTART := NOW(); ExportName :='KMSF.UKFinHlthProfSpclty'; DROP TABLE KMSF.UKFinHlthProfSpclty IF EXISTS; STATEMENT = 'CREATE  TABLE ' || ExportName || '   AS
					(SELECT  hps.HlthProfObjId AS HlthProfObjId 
					,spclty.CrossRefCd		 AS SpcltyCdVal 
					,spclty.Mne				 AS SpcltyCdMne 
					,spclty.DescText		 AS SpcltyCdDesc 
					FROM KMSF.TNX__DBO__HLTHPROFSPCL hps	 
					JOIN KMSF.DSS__SMSDSS__CDVALUES_SPECIALTY AS spclty ON hps.SpclCd = spclty.IntrnCd 
					WHERE hps.EXP_END_DATE = ''9999-12-31'')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_040_LoadHlthProfSpcltyTbl'); SUBSTART := NOW(); ExportName :='KMSF.UKFinSfBudgetGCR'; DROP TABLE KMSF.UKFinSfBudgetGCR if exists; STATEMENT = 'CREATE  TABLE '|| ExportName || '  AS
					(SELECT 
					  CASE WHEN gl.chg_type = 1 THEN cdv.EncMRN ELSE cdv2.EncMrn END  AS EncMRN 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.EcdNo ELSE cdv2.EcdNo END	AS EcdNo 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.EncTypeMne ELSE cdv2.EncTypeMne END AS EncTypeMne 																										 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.EncLocId ELSE cdv2.EncLocId END AS EncLocId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.ApmSchedLocCd ELSE cdv2.ApmSchedLocCd END AS ApmSchedLocCdAS
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.ChgObjId ELSE cdv2.ChgObjId END AS ChgObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.GlEntObjId ELSE cdv2.GlEntObjId END AS ChgGlEntObjId 
					  ,CASE WHEN gl.chg_type = 2 THEN fin.GlEntObjId ELSE NULL END AS FinGLEntObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN CAST(cdv.StrDate AS DATE) ELSE CAST(cdv2.StrDate AS DATE) END  AS StrDate 
					  ,pd.FsclYr AS FsclYr 
					  ,pd.FsclPer AS FlscPer 
					  ,pd.FsclYrPer	 AS FsclYrPer 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SupvHlthProfObjId ELSE cdv2.SupvHlthProfObjId END	 AS SupvHlthProfObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SupvHlthProfNpi ELSE cdv2.SupvHlthProfNpi END		 AS SupvHlthProfNpi 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SupvHlthProfName ELSE cdv2.SupvHlthProfName END	 AS SupvHlthProfName 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SvcProvHPOObjId ELSE cdv2.SvcProvHPOObjId END		 AS SvcProvHPOObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SvcProvShortName ELSE cdv2.SvcProvShortName END	 AS SvcProvShortName 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.DivHpoObjId ELSE cdv2.DivHpoObjId END					 AS DivHpoObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.DivShortName ELSE cdv2.DivShortName END				 AS DivShortName 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.BusUnitHpoObjId ELSE cdv2.BusUnitHpoObjId END		 AS BusUnitHpoObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.BusUnitShortName ELSE cdv2.BusUnitShortName END	 AS BusUnitShortName 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.ChgProcCdVal ELSE cdv2.ChgProcCdVal END				 AS ChgProcCdVal 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.ChgProcCdDesc ELSE cdv2.ChgProcCdDesc END				 AS ChgProcCdDesc 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SvcObjId ELSE cdv2.SvcObjId END							 AS SvcObjId 
					  ,CASE WHEN gl.chg_type = 1 THEN cdv.SvcBlName ELSE cdv2.SvcBlName END							 AS SvcBlName 
					  ,CASE WHEN gl.chg_type = 1 THEN svc.RptGrp1Mne ELSE svc2.RptGrp1Mne END					 AS ServiceGroup1 
					  ,CASE WHEN gl.chg_type = 1 THEN svc.RptGrp2Mne ELSE svc2.RptGrp2Mne END					 AS ServiceGroup2 
					  ,gl.sap_gl 	 AS GL 
					  ,glDesc.sap_gl_Desc AS GLDesc 
					  ,CASE WHEN gl.chg_type = 1 THEN amount ELSE 0 END	AS ChgAmt 
					  ,CASE WHEN gl.chg_type = 2 THEN amount * -1 ELSE 0 END AS PmtAmt
					  ,CASE WHEN gl.chg_type = 1 THEN ''Charge'' ELSE ''Payment'' END AS TransType
					  ,NULL	 AS PyrObjId 
					  , NULL AS PyrMne 
					  ,NULL	 AS PyrShortName
					from KMSF.DSS__SMSDSS__KMSF_SAP_GL_ASSN gl
					  join KMSF.DSS__SMSDSS__KMSF_SAP_GL glDesc on gl.sap_gl = glDesc.sap_gl and glDesc.EXP_END_DATE =''9999-12-31''
					  left join KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD_DATE pd on CAST(gl.sf_work_date AS DATE) = CAST(pd.fullDate AS DATE) and pd.EXP_END_DATE =''9999-12-31''
					  left join KMSF.DSS__SMSDSS__FINTRANSFCTV fin on gl.chg_type = 2 and gl.GLEntObjId = fin.GLEntObjId 
					  left join KMSF.DSS__SMSDSS__CHGFCTV chg on gl.chg_type = 1 and gl.GLEntObjId = chg.GLEntObjId 
					  left join KMSF.DSS__SMSDSS__CHGFCTV chg2 on gl.chg_type = 2 and fin.ChgObjId = chg2.ChgObjId and chg2.TransTypeCd = 1
					  left join KMSF.DSS__SMSDSS__SVCMSTRV svc on gl.chg_type = 1 and chg.SvcObjId = svc.SvcObjId
					  left join KMSF.DSS__SMSDSS__SVCMSTRV svc2 on gl.chg_type = 2 and chg2.SvcObjId = svc2.SvcObjId
					  left join KMSF.DSS__CUSTOMER__KMSFSFCHGDTLV cdv ON gl.chg_type = 1 AND chg.GlEntObjId = cdv.GlEntObjId
					  left join KMSF.DSS__CUSTOMER__KMSFSFCHGDTLV cdv2 ON gl.chg_type = 2 AND chg2.GlEntObjId = cdv2.GlEntObjId
					where sf_work_date >= '''||FsclStartDate || ''' and  sf_work_date<= ''' || FsclStopDate || '''
					  and processed_ind = 1 AND gl.EXP_END_DATE=''9999-12-31'')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_050_LoadSfBudgetGCRTbl'); SUBSTART := NOW(); ExportName :='KMSF.UKFinSfVolumeMonthly'; DROP TABLE KMSF.UKFinSfVolumeMonthly IF EXISTS; STATEMENT = 'CREATE  TABLE ' || ExportName || '  AS 
					(SELECT 
					   cdv.EncMRN                 AS EncMRN  
					  ,cdv.EcdNo				  AS EcdNo  
					  ,cdv.EncTypeMne			  AS EncTypeMne  
					  ,cdv.EncLocId				  AS EncLocId  
					  ,cdv.EncProvHPOObjId	      AS EncProvHPOObjId   
					  ,cdv.EncProvName		  AS EncProvName  
					  ,cdv.APMSCHEDLOCCD		  AS ApmSchedLocCd  
					  ,cdv.ChgObjId				  AS ChgObjId  
					  ,cdv.GlEntObjId			  AS GlEntObjId  
					  ,TO_CHAR(cdv.StrDate, ''MM/DD/YYYY HH:MI:SS AM'') AS StrDate  
					  ,apd.FsclYr				  AS FsclYr  
					  ,apd.FsclPer				  AS FsclPer   
					  ,cdv.SupvHlthProfObjId      AS SupvHlthProfObjId  
					  ,cdv.SUPVHLTHPROFNPI    AS SupvHlthProfNpi  
					  ,cdv.SUPVHLTHPROFNAME      AS SupvHlthProfName  
					  ,cdv.SvcProvHPOObjId	      AS SvcProvHPOObjId  
					  ,cdv.SVCPROVSHORTNAME		  AS SVCPROVSHORTNAME  
					  ,cdv.DIVHPOOBJID				  AS DIVHPOOBJID  
					  ,cdv.DIVSHORTNAME			  AS DivShortName  
					  ,cdv.BUSUNITHPOOBJID	      AS BUSUNITHPOOBJID    
					  ,cdv.BUSUNITSHORTNAME		  AS BUSUNITSHORTNAME  
					  ,cdv.ChgProcCdVal			  AS ChgProcCdVal  
					  ,cdv.CHGPROCCDDESC		  AS ChgProcCdDesc  
					  ,cdv.ChgModfVal			  AS ChgModfVal  
					  ,CASE WHEN CHG.WORK_RVU <1 AND CHG.WORK_RVU>-1 THEN TO_CHAR(CHG.WORK_RVU,''FM0D90'') ELSE TRIM(LEADING ''0'' FROM to_char(chg.work_rvu,''FM999999D90'')) END AS work_rvu  
					  ,CASE WHEN cdv.TtlAdjWorkRvu <1 AND cdv.TtlAdjWorkRvu>-1 THEN TO_CHAR(cdv.TtlAdjWorkRvu,''FM0D90'') ELSE TRIM(LEADING ''0'' FROM to_char(cdv.TtlAdjWorkRvu,''FM999999D90'')) END  AS TtlAdjWorkRvu  
					  ,CASE WHEN cdv.Qty <1 AND cdv.Qty>-1 THEN TO_CHAR(cdv.Qty,''FM0D90'') ELSE TRIM(LEADING ''0'' FROM to_char(cdv.Qty,''FM999999D90'')) END AS Qty  
					  ,CASE WHEN cdv.ExtPriceAmt <1 AND cdv.Qty>-1 THEN TO_CHAR(cdv.ExtPriceAmt,''FM0D90'') ELSE TRIM(LEADING ''0'' FROM to_char(cdv.ExtPriceAmt,''FM999999D90'')) END  AS ExtPriceAmt 
					  ,cdv.PRIMPYROBJID			  AS PrimPyrObjId 
					  ,cdv.PrimPyrMne				  AS PrimPyrMne 
					  ,cdv.PrimPyrShortName		  AS PrimPyrShortName 
					  ,cdv.CurrPyrObjId			  AS CurrPyrObjId 
					  ,cdv.CurrPyrMne				  AS CurrPyrMne 
					  ,cdv.CurrPyrShortName		  AS CurrPyrShortName 
					  ,gl.cost_center			  AS CostCenter 
					  ,gl.fund_id				  AS FundId 
					  ,gl.sap_gl				  AS SAPGL 
					  ,cdv.SvcSysGenId			  AS SvcSysGenId 
					  ,cdv.PosCd as PosCd 
  					  ,cdv.PosDesc as PosDesc 
  					  ,SupHlthProf.TypeMne as SupvHlthProfTypeMne
					  ,SupvNPIDtl.HlthProfPriHQPMNo as SupvHlthProvHQPMNo
					  ,cdv.PerfHlthProfObjId  as PerfHlthProfObjId 
					  ,PerfNPIDtl.HlthProfName  as PerfHlthProfName 
					  ,PerfNPIDtl.HlthProfNPI as PerfHlthProfNPI 
					  ,PerfNPIDtl.HlthProfPriHQPMNo as PerfHlthProfHQPM 
					  ,SupHlthProf.TypeMne as PerfHlthProfTypeMne 
					  ,cdv.ELShortName as ELShortName 
					  ,cdv.AnesPhysUnits as AnesPhysUnits 
					  ,cdv.AnesProcDurMins  as AnesProcDurMins 
					  ,cdv.AnesProcBaseUnits as AnesProcBaseUnits 
					  ,TO_CHAR( cdv.AnesStartDtime,''MM/DD/YYYY HH:MI:SS AM'') as AnesStartDtime 
					  ,TO_CHAR( cdv.AnesStopDtime, ''MM/DD/YYYY HH:MI:SS AM'') as AnesStopDtime 
					  ,cdv.AnesSurgProcCd as AnesSurgProcCd 
					  ,cdv.AnesTimeUnits as AnesTimeUnits 
					  ,cdv.AnesTotUnits as AnesTotUnits 
					  ,cdv.AnesModVal1 as AnesModVal1 
					  ,cdv.AnesModVal2 as AnesModVal2 
					  ,cdv.AnesModVal3 as AnesModVal3 
					  ,cdv.AnesModVal4 as AnesModVal4 
					  ,cdv.AnesModVal5 as AnesModVal5 
					  ,TO_CHAR( cdv.RvsePostDate ,''MM/DD/YYYY HH:MI:SS AM'') as RvsePostDate 
					  ,cdv.TransTypeMne as TransTypeMne 
					  ,cast(null as varchar(10)) as BadDebtFlag 
					  ,cdv.PrimPyrHlthPlanRptGrpMne as PrimPyrHlthPlanRptGrpMne 
					  ,cast(null as varchar(30))as PrimPyrPmtAmt 
					  ,cast(null as varchar(30)) as PrimPyrAdjAmt 
					  ,cast(null as varchar(30))as PrimPyrBalAmt 
					  ,cdv.ROShortName as RcvOwnerShortName 
					FROM KMSF.DSS__CUSTOMER__KMSFSFCHGDTLV cdv 
					  JOIN KMSF.DSS__SMSDSS__KMSF_SF_CHG chg ON chg.GlEntObjId = cdv.GlEntObjId 
					  JOIN KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD_DATE apd ON CAST(cdv.RptPostBusDate AS DATE) = CAST(apd.FullDate AS DATE) 
					  LEFT JOIN KMSF.DSS__SMSDSS__KMSF_SAP_GL_ASSN gl ON gl.chg_type = 1 AND gl.GLEntObjId = cdv.GLEntObjId AND gl.current_record = 1 
					  LEFT JOIN KMSF.DSS__SMSDSS__HLTHPROFMSTRV SupHlthProf ON SupHlthProf.HlthProfObjId = cdv.SupvHlthProfObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__HLTHPROFMSTRV PerfHlthProf ON PerfHlthProf.HlthProfObjId = cdv.PerfHlthProfObjId
					  LEFT JOIN KMSF.DSS__CUSTOMER__KMSFHLTHPROFNPIDTL SupvNPIDtl ON SupvNPIDtl.HlthProfNPI = cdv.SupvHlthProfNPI
                      LEFT JOIN KMSF.DSS__CUSTOMER__KMSFHLTHPROFNPIDTL PerfNPIDtl ON PerfNPIDtl.HlthProfNPI = cdv.PerfHlthProfNPI
					WHERE cdv.RptPostBusDate BETWEEN ''' || VMFsclStartDate || '''  AND  ''' || VMFsclStopDate || ''')'; EXECUTE IMMEDIATE STATEMENT; DROP TABLE KMSF.TEMP2 IF EXISTS; 
					CREATE TABLE KMSF.TEMP2 AS (
					  WITH TEMP1 AS (SELECT M.GLENTOBJID
					, SUM(coalesce((CASE WHEN F1.PrimPyrRcvInd =1 THEN F.TOTPAYAMT ELSE 0.00 END), 0.00)) AS PrimPyrPmtAmt
					, SUM(coalesce((CASE WHEN F1.PrimPyrRcvInd =1 THEN F.TOTADJAMT ELSE 0.00 END), 0.00)) AS PrimPyrAdjAmt
					, SUM(coalesce((CASE WHEN F1.PrimPyrRcvInd =1 THEN F.BALAMT ELSE 0.00 END), 0.00)) AS PrimPyrBalAmt 
					,CASE WHEN SUM(coalesce((case when F1.RcvBlStsMne = 'BD' then 1 else 0 end),0))=0 THEN 'N' else 'Y' end AS BadDebtFlag FROM KMSF.UKFINSFVOLUMEMONTHLY M LEFT JOIN KMSF.DSS__SMSDSS__RCVRPT F ON M.ChgObjId=F.ChgObjId AND F.PrcsStsCd=1 JOIN KMSF.DSS__SMSDSS__RCVFCT F1 ON (F.RCVSRCOBJID = F1.RCVSRCOBJID) GROUP BY M.GLENTOBJID )
					SELECT GLENTOBJID
						,CASE WHEN PrimPyrPmtAmt >-1 AND PrimPyrPmtAmt <1 THEN TO_CHAR(PrimPyrPmtAmt, 'FM0D90') ELSE TRIM(LEADING '0' FROM TO_CHAR(PrimPyrPmtAmt, 'FM99999999D90')) END AS PrimPyrPmtAmt
						,CASE WHEN PrimPyrAdjAmt >-1 AND PrimPyrAdjAmt <1 THEN TO_CHAR(PrimPyrAdjAmt, 'FM0D90') ELSE TRIM(LEADING '0' FROM TO_CHAR(PrimPyrAdjAmt, 'FM99999999D90')) END AS PrimPyrAdjAmt
						,CASE WHEN PrimPyrBalAmt >-1 AND PrimPyrBalAmt <1 THEN TO_CHAR(PrimPyrBalAmt, 'FM0D90') ELSE TRIM(LEADING '0' FROM TO_CHAR(PrimPyrBalAmt, 'FM99999999D90')) END AS PrimPyrBalAmt
					  ,BadDebtFlag FROM TEMP1
					
					); 
					
					MERGE INTO KMSF.UKFINSFVOLUMEMONTHLY AS A using kmsf.TEMP2 AS B ON A.GLENTOBJID = B.GLENTOBJID WHEN MATCHED THEN UPDATE SET A.PrimPyrPmtAmt=B.PrimPyrPmtAmt, A.PrimPyrAdjAmt=B.PrimPyrAdjAmt , A.PrimPyrBalAmt=B.PrimPyrBalAmt, A.BadDebtFlag = B.BadDebtFlag; 
					
					UPDATE KMSF.UKFINSFVOLUMEMONTHLY SET PrimPyrPmtAmt=COALESCE(PrimPyrPmtAmt,'0.00'), PrimPyrAdjAmt=COALESCE(PrimPyrAdjAmt,'0.00'), PrimPyrBalAmt=COALESCE(PrimPyrBalAmt,'0.00'), BadDebtFlag=COALESCE(BadDebtFlag,'N'); INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl'); SUBSTART := NOW(); DROP TABLE KMSF.UKFinWorkRvuDtl IF EXISTS; CREATE TABLE KMSF.UKFinWorkRvuDtl AS ( select fs.acct_yr AS FlscYr ,fs.acct_pd AS FsclPer ,fs.acct_yr_pd AS FsclYrPer ,COALESCE(Div.DeptHPOShortName,Dept.DeptHPOShortName,'Sig-' || h.lvl4_name) AS BusUnitShortName ,COALESCE(Div.DivHPOShortName,'Sig-' || h.lvl5_name) AS lvl5_id ,sum(fs.work_rvu) AS TotAdjWorkRvu ,'Sig' AS RowSrce from KMSF.DSS__SMSDSS__KMSF_FIN_SUMM fs left join KMSF.DSS__SMSDSS__KMSF_RPT_HIER h on h.prov_id = fs.orgz_cd and h.EXP_END_DATE='9999-12-31' LEFT JOIN KMSF.DSS__CUSTOMER__KMSFSFSIGDEPTTOSFBUSUNITXWALK Dept ON fs.lvl4_id = Dept.lvl4_id AND Dept.EXP_END_DATE='9999-12-31' LEFT JOIN KMSF.DSS__CUSTOMER__KMSFSFSIGDIVTOSFBUSUNITDIVXWALK Div ON fs.lvl5_id = Div.lvl5_id AND Div.EXP_END_DATE='9999-12-31' where fs.acct_yr_pd = FsclYrPerV and fs.work_rvu is not null and fs.EXP_END_DATE='9999-12-31' group by fs.acct_yr ,fs.acct_pd ,fs.acct_yr_pd ,COALESCE(Div.DeptHPOShortName,Dept.DeptHPOShortName,'Sig-' || h.lvl4_name) ,COALESCE(Div.DivHPOShortName,'Sig-' || h.lvl5_name) having sum(fs.work_rvu) <> 0 UNION SELECT CYTD.acct_yr AS FlscYr ,CYTD.acct_pd as FsclPer ,CYTD.acct_yr_pd AS FsclYrPer ,CYTD.Department AS BusUnitShortName ,CYTD.Division AS DivShortName ,sum(NVL(CYTD.work_rvu, 0)) AS TotAdjWorkRvu ,'SF' AS RowSrce FROM KMSF.DSS__CUSTOMER__CHARGES_YTD CYTD LEFT JOIN KMSF.DSS__SMSDSS__KMSF_ACCT_PD_DESC pd ON CYTD.acct_pd = LPAD( pd.acct_pd,2,'0') AND pd.EXP_END_DATE='9999-12-31' LEFT JOIN (select distinct Div_HpoObjId AS DivHpoObjId,Div_Short_Name AS DivShortName from KMSF.DSS__SMSDSS__KMSF_SF_SP_HIER_V) d1 ON d1.DivShortName = CYTD.Division WHERE CYTD.acct_yr_pd = FsclYrPerV AND NVL(CYTD.work_rvu,0) <> 0 and CYTD.EXP_END_DATE='9999-12-31' GROUP BY CYTD.acct_yr ,CYTD.acct_pd ,CYTD.acct_yr_pd ,CYTD.Department ,CYTD.Division HAVING sum(NVL(CYTD.work_rvu, 0)) <> 0); INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl'); INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOG (LOGENTRYID, REPROCOPT, FSCLYRPER, STARTDTIME,ENDDTIME, RUNDTIME, RSLTCD, RSLTTXT) VALUES (LOGSEQ, ReProcOptV, FsclYrPerV, DATE(StartDtimeV), NOW(), NOW-StartDtimeV, 1 ,'Success') ; RETURN 0; END; 
END_PROC;

