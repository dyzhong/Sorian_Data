CREATE OR REPLACE PROCEDURE KMSF.KMSFUKFINDATADUMPSP_MASTER2()
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
 declare ReProcOptV INTEGER; FsclYrPerV VARCHAR(6) ; FsclStartDate VARCHAR(8) ; FsclStopDate VARCHAR(8) ; VMFsclStartDate VARCHAR(8) ; VMFsclStopDate VARCHAR(8) ; ExportPath VARCHAR(255) ; ExportName VARCHAR(255) ; StartDtimeV DATETIME ; EndDtimeV DATETIME ; RsltCdV BYTEINT ; RsltTxtV varchar(255) ; FsclYrPerRd RECORD; REC RECORD; LOGSEQ BIGINT; MAXACCTFCLYER RECORD; ACCTYERV VARCHAR(6); DATERANGEYEARPER RECORD; FLAGV INTEGER; STATEMENT VARCHAR(5000); SUBSTART TIMESTAMP; BEGIN StartDtimeV := NOW(); ReProcOptV := NVL(ReProcOptV,0); FLAGV :=0; DROP TABLE KMSF.FSCLYRPERRDT IF EXISTS; CREATE TABLE KMSF.FSCLYRPERRDT (FSCLYRPERMIN) AS (SELECT MIN(FsclYrPer) From KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD WHERE FSCLYRPER > NVL((SELECT MAX(FSCLYRPER) FROM KMSF.KMSFUKFINDATADUMPPROCLOG WHERE NVL(RSLTCD,0) = 1),'201605') AND date(CAST(FSCLPEREND AS DATE))+5 < DATE(now())); FOR REC IN SELECT * FROM KMSF.FSCLYRPERRDT LOOP; IF REC.FSCLYRPERMIN IS NULL THEN RAISE NOTICE 'NO FSCLYRPERMIN FOUND'; RETURN 1; ELSE FsclYrPerV = REC.FSCLYRPERMIN; RAISE NOTICE '%', REC.FSCLYRPERMIN; END IF; END LOOP; DELETE FROM KMSF.KMSFUKFINDATADUMPPROCLOGDTL WHERE LOGENTRYID IN (SELECT LOGENTRYID FROM KMSF.KMSFUKFINDATADUMPPROCLOG WHERE FSCLYRPER = FsclYrPerV); DELETE FROM KMSF.KMSFUKFINDATADUMPPROCLOG WHERE FSCLYRPER = FsclYrPerV; FOR MAXACCTFCLYER IN SELECT NEXT VALUE FOR KMSF.KMSFUKFINDATADUMPPROCLOG_SEQ AS SEQN LOOP; LOGSEQ = MAXACCTFCLYER.SEQN; END LOOP; IF NVL(FsclYrPerV, '') = '' THEN FLAGV :=1; RSLTTXTV = 'Process Failed - No Fiscal Period Was Passed'; ELSIF FsclYrPerV <'201512' THEN FLAGV :=1; RsltTxtV = 'Process Failed - The Minimum Fiscal Year Period Was Violated'; ELSE FOR MAXACCTFCLYER IN SELECT MAX(FsclYrPer) AS MAXDATE FROM KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD WHERE EXP_END_DATE='9999-12-31' AND date(CAST(FSCLPEREND AS DATE))+5 < DATE(NOW()) LOOP; IF FOUND THEN ACCTYERV = MAXACCTFCLYER.MAXDATE; ELSE FLAGV :=1; RsltTxtV = 'Process Failed - The Maximum Fiscal Year Period Was Violated'; END IF; END LOOP; END IF; IF FLAGV = 1 THEN INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOG (LOGENTRYID, REPROCOPT, FSCLYRPER, STARTDTIME, ENDDTIME, RSLTCD, RSLTTXT) VALUES (LOGSEQ, ReProcOptV, FsclYrPerV, DATE(StartDtimeV), NOW(), 0, RsltTxtV ) ; RETURN 1; END IF ; FOR DATERANGEYEARPER IN SELECT CAST(REPLACE(FSCLPERSTART,'-','') AS VARCHAR(10)) AS FSCLPERSTART, CAST(REPLACE(FSCLPEREND,'-','') AS VARCHAR(10)) AS FSCLPEREND, CASE WHEN ADD_MONTHS(CAST(FSCLPERSTART AS DATE), -5) <'2015-12-01' THEN '20151201' ELSE CAST(REPLACE(ADD_MONTHS(CAST(FSCLPERSTART AS DATE), -5),'-','') AS VARCHAR(10)) END AS VMFSTART FROM KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD WHERE EXP_END_DATE ='9999-12-31' AND FSCLYRPER=FsclYrPerV LOOP; IF FOUND THEN FsclStartDate = DATERANGEYEARPER.FSCLPERSTART ; FsclStopDate =DATERANGEYEARPER.FSCLPEREND ; VMFsclStartDate = DATERANGEYEARPER.VMFSTART ; VMFsclStopDate =DATERANGEYEARPER.FSCLPEREND ; END IF; END LOOP; RSLTCDV :=0; ExportPath := '/export/home/etluser/UK_FINANCIAL_EXTRACT/' ; SUBSTART := NOW(); ExportName := 'KMSF.UKFinBusUnitMstr'; DROP TABLE KMSF.UKFinBusUnitMstr IF EXISTS; STATEMENT := 'CREATE  TABLE ' || ExportName || ' AS ( SELECT  HPOObjId AS BusUnitHpoObjId ,ShortName AS BusUnitShortName  FROM KMSF.DSS__SMSDSS__HPOMSTRV WHERE TypeMne = ''Bus Unit'')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl'); SUBSTART := NOW(); DROP TABLE KMSF.UKFinDivMstr IF EXISTS; ExportName :='KMSF.UKFinDivMstr'; STATEMENT := 'CREATE  TABLE '|| ExportName || ' AS ( SELECT  HPOObjId AS DivHpoObjId ,ShortName AS DivShortName  FROM KMSF.DSS__SMSDSS__HPOMSTRV WHERE TypeMne = ''Division'')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl'); SUBSTART := NOW(); DROP TABLE KMSF.UKFinHlthProfMstr IF EXISTS; ExportName :='KMSF.UKFinHlthProfMstr'; STATEMENT = 'CREATE  TABLE ' || ExportName || ' AS
				(SELECT  hp.HlthProfObjId AS HlthProfObjId 
				  ,hp.HlthProfNPI AS HlthProfNPI 
				  ,hp.DegreeText   AS DegreeText 
				  ,hp.GivenName		AS FirstName 
				  ,hp.MiddleName   AS MiddleName 
				  ,hp.FamName		AS LastName 
				  ,NPIDTL.HLTHPROFPRITXNMYCD AS SPCLTYCDVAL
				  ,SPCLTY.MNE AS SPCLTYCDMNE
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
					   efv.EncMRN                 AS EncMRN  
					  ,efv.EcdNo				  AS EcdNo  
					  ,efv.EncTypeMne			  AS EncTypeMne  
					  ,efv.EncLocId				  AS EncLocId  
					  ,efv.EncProvHPOObjId	      AS EncProvHPOObjId  
					  ,efv.EncProvHPOTypeMne      AS EncProvHPOTypeMne  
					  ,EncProv.ShortName		  AS EncProvName  
					  ,EncUsrData.textval		  AS ApmSchedLocCd  
					  ,cfv.ChgObjId				  AS ChgObjId  
					  ,cfv.GlEntObjId			  AS GlEntObjId  
					  ,CAST(cfv.StrDate AS DATE)  AS StrDate  
					  ,apd.FsclYr				  AS FsclYr  
					  ,apd.FsclPer				  AS FsclPer  
					  ,apd.FsclYrPer			  AS FsclYrPer  
					  ,cfv.SupvHlthProfObjId      AS SupvHlthProfObjId  
					  ,SupHlthProf.HlthProfNPI    AS SupvHlthProfNpi  
					  ,SupHlthProf.RptName	      AS SupvHlthProfName  
					  ,cfv.SvcProvHPOObjId	      AS SvcProvHPOObjId  
					  ,SvcProv.ShortName		  AS SvcProvShortName  
					  ,Div.HpoObjId				  AS DivHpoObjId  
					  ,Div.ShortName			  AS DivShortName  
					  ,BusUnit.HpoObjId		      AS BusUnitHpoObjId    
					  ,BusUnit.ShortName		  AS BusUnitShortName  
					  ,cfv.ChgProcCdVal			  AS ChgProcCdVal  
					  ,cpt.Mne					  AS ChgProcCdDesc  
					  ,cfv.ChgModfVal			  AS ChgModfVal  
					  ,cfv.SvcObjId				  AS SvcObjId  
					  ,smv.BlName				  AS SvcBlName  
					  ,chg.work_rvu				  AS work_rvu  
					  ,chg.tot_adj_work_rvu		  AS TtlAdjWorkRvu  
					  ,cfv.Qty					  AS Qty  
					  ,cfv.ExtPriceAmt			  AS ExtPriceAmt 
					  ,PrimPyr.PyrObjId			  AS PrimPyrObjId 
					  ,PrimPyr.Mne				  AS PrimPyrMne 
					  ,PrimPyr.ShortName		  AS PrimPyrShortName 
					  ,CurrPyr.PyrObjId			  AS CurrPyrObjId 
					  ,CurrPyr.Mne				  AS CurrPyrMne 
					  ,CurrPyr.ShortName		  AS CurrPyrShortName 
					  ,cfv.RvsePostDate			  AS RvsePostDate 
					  ,cfv.TransTypeMne			  AS TransTypeMne 
					  ,gl.cost_center			  AS CostCenter 
					  ,gl.fund_id				  AS FundId 
					  ,gl.sap_gl				  AS SAPGL 
					  ,smv.SvcSysGenId			  AS SvcSysGenId 	
					FROM KMSF.DSS__SMSDSS__CHGFCTV cfv
					  JOIN KMSF.DSS__CUSTOMER__KMSFSFCHGDTLV cdv on cdv.Glentobjid = cfv.Glentobjid
					  JOIN KMSF.DSS__SMSDSS__KMSF_SF_CHG chg ON chg.GlEntObjId = cfv.GlEntObjId AND chg.EXP_END_DATE =''9999-12-31''
					  JOIN KMSF.DSS__SMSDSS__KMSF_SF_ACCT_PD_DATE apd ON CAST(cfv.RptPostBusDate AS DATE) = CAST(apd.FullDate AS DATE) AND apd.EXP_END_DATE=''9999-12-31''
					  LEFT JOIN KMSF.DSS__SMSDSS__ENCFCTV efv ON efv.EncObjId = cfv.EncObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__HPOMSTRV EncProv ON EncProv.HPOObjId = efv.EncProvHPOObjId
					  LEFT JOIN KMSF.TNX__DBO__UDTENCUSERDATA EncUsrData ON efv.EncObjId = EncUsrData.EncObjId and EncUsrData.MetaDataObjId = 1 AND EncUsrData.EXP_END_DATE =''9999-12-31''
					  LEFT JOIN KMSF.DSS__SMSDSS__HLTHPROFMSTRV SupHlthProf ON SupHlthProf.HlthProfObjId = cfv.SupvHlthProfObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__CLASFMSTRV cpt ON cpt.CdVal = cfv.ChgProcCdVal AND cpt.ClasfTypeMne = ''Proc''
					  LEFT JOIN KMSF.DSS__SMSDSS__SVCMSTRV smv ON smv.SvcObjId = cfv.SvcObjId 
					  LEFT JOIN KMSF.DSS__SMSDSS__BLPERFCTV AS bpfv ON cfv.BlPerObjId = bpfv.BlPerObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__PYRHLTHPLANMSTRV AS PrimHlthPlan ON bpfv.PrimPyrHlthPlanObjId = PrimHlthPlan.PyrHlthPlanObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__PYRMSTRV PrimPyr ON PrimPyr.PyrobjId = PrimHlthPlan.PyrobjId
					  LEFT JOIN KMSF.DSS__SMSDSS__PYRHLTHPLANMSTRV AS CurrHlthPlan ON bpfv.CurrHlthPlanObjId = CurrHlthPlan.PyrHlthPlanObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__PYRMSTRV CurrPyr ON CurrPyr.PyrobjId = CurrHlthPlan.PyrobjId
					  LEFT JOIN KMSF.DSS__SMSDSS__HPOMSTRV SvcProv ON SvcProv.HPOObjId = cfv.SvcProvHPOObjID
					  LEFT JOIN KMSF.DSS__SMSDSS__HPOMSTRV Div ON Div.HPOObjId = SvcProv.ParentHPOObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__HPOMSTRV BusUnit ON BusUnit.HpoObjId = Div.ParentHPOObjId
					  LEFT JOIN KMSF.DSS__SMSDSS__KMSF_SAP_GL_ASSN gl ON gl.chg_type = 1 AND gl.GLEntObjId = cfv.GLEntObjId AND gl.current_record = 1 AND  gl.EXP_END_DATE =''9999-12-31''
					WHERE cdv.RptPostBusDate BETWEEN ''' || VMFsclStartDate || '''  AND  ''' || VMFsclStopDate || ''')'; EXECUTE IMMEDIATE STATEMENT; INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl'); SUBSTART := NOW(); DROP TABLE KMSF.UKFinWorkRvuDtl IF EXISTS; CREATE TABLE KMSF.UKFinWorkRvuDtl AS ( select fs.acct_yr AS FlscYr ,fs.acct_pd AS FsclPer ,fs.acct_yr_pd AS FsclYrPer ,COALESCE(Div.DeptHPOShortName,Dept.DeptHPOShortName,'Sig-' || h.lvl4_name) AS BusUnitShortName ,COALESCE(Div.DivHPOShortName,'Sig-' || h.lvl5_name) AS lvl5_id ,sum(fs.work_rvu) AS TotAdjWorkRvu ,'Sig' AS RowSrce from KMSF.DSS__SMSDSS__KMSF_FIN_SUMM fs left join KMSF.DSS__SMSDSS__KMSF_RPT_HIER h on h.prov_id = fs.orgz_cd and h.EXP_END_DATE='9999-12-31' LEFT JOIN KMSF.DSS__CUSTOMER__KMSFSFSIGDEPTTOSFBUSUNITXWALK Dept ON fs.lvl4_id = Dept.lvl4_id AND Dept.EXP_END_DATE='9999-12-31' LEFT JOIN KMSF.DSS__CUSTOMER__KMSFSFSIGDIVTOSFBUSUNITDIVXWALK Div ON fs.lvl5_id = Div.lvl5_id AND Div.EXP_END_DATE='9999-12-31' where fs.acct_yr_pd = FsclYrPerV and fs.work_rvu is not null and fs.EXP_END_DATE='9999-12-31' group by fs.acct_yr ,fs.acct_pd ,fs.acct_yr_pd ,COALESCE(Div.DeptHPOShortName,Dept.DeptHPOShortName,'Sig-' || h.lvl4_name) ,COALESCE(Div.DivHPOShortName,'Sig-' || h.lvl5_name) having sum(fs.work_rvu) <> 0 UNION SELECT CYTD.acct_yr AS FlscYr ,CYTD.acct_pd as FsclPer ,CYTD.acct_yr_pd AS FsclYrPer ,CYTD.Department AS BusUnitShortName ,CYTD.Division AS DivShortName ,sum(NVL(CYTD.work_rvu, 0)) AS TotAdjWorkRvu ,'SF' AS RowSrce FROM KMSF.DSS__CUSTOMER__CHARGES_YTD CYTD LEFT JOIN KMSF.DSS__SMSDSS__KMSF_ACCT_PD_DESC pd ON CYTD.acct_pd = LPAD( pd.acct_pd,2,'0') AND pd.EXP_END_DATE='9999-12-31' LEFT JOIN (select distinct Div_HpoObjId AS DivHpoObjId,Div_Short_Name AS DivShortName from KMSF.DSS__SMSDSS__KMSF_SF_SP_HIER_V) d1 ON d1.DivShortName = CYTD.Division WHERE CYTD.acct_yr_pd = FsclYrPerV AND NVL(CYTD.work_rvu,0) <> 0 and CYTD.EXP_END_DATE='9999-12-31' GROUP BY CYTD.acct_yr ,CYTD.acct_pd ,CYTD.acct_yr_pd ,CYTD.Department ,CYTD.Division HAVING sum(NVL(CYTD.work_rvu, 0)) <> 0); INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOGDTL VALUES (LOGSEQ, SUBSTART, NOW(), NOW()-SUBSTART,'KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl'); INSERT INTO KMSF.KMSFUKFINDATADUMPPROCLOG (LOGENTRYID, REPROCOPT, FSCLYRPER, STARTDTIME,ENDDTIME, RUNDTIME, RSLTCD, RSLTTXT) VALUES (LOGSEQ, ReProcOptV, FsclYrPerV, DATE(StartDtimeV), NOW(), NOW-StartDtimeV, 1 ,'Success') ; RETURN 0; END; 
END_PROC;

