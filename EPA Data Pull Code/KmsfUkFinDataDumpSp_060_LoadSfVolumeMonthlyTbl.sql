USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl]    Script Date: 11/30/2017 9:52:30 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@VMFsclStartDate DATE = NULL
    ,@VMFsclStopDate DATE = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl','')

SET NOCOUNT ON

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

IF OBJECT_ID('Staging.UKFinSfVolumeMonthly','U') IS NOT NULL
	DROP TABLE Staging.UKFinSfVolumeMonthly
SELECT EncMRN = cdv.EncMRN  --1
  ,EcdNo = cdv.EcdNo  --2
  ,EncTypeMne = cdv.EncTypeMne  --3

  ,EncLocId = cdv.EncLocId  --4
  ,EncProvHPOObjId = cdv.EncProvHPOObjId  --5
  ,EncProvName = cdv.EncProvName  --6
  ,ApmSchedLocCd = cdv.ApmSchedLocCd  --7

  ,ChgObjId = cdv.ChgObjId  --8
  ,GlEntObjId = cdv.GlEntObjId  --9

  ,StrDate = CAST(cdv.StrDate AS DATE)  --10

  ,FsclYrDOE = apd.FsclYr  --11
  ,FsclPerDOE = apd.FsclPer  --12

  ,SupvHlthProfObjId = cdv.SupvHlthProfObjId  --13
  ,SupvHlthProfNpi = cdv.SupvHlthProfNPI  --14
  ,SupvHlthProfName = cdv.SupvHlthProfName  --15

  ,SvcProvHPOObjId = cdv.SvcProvHPOObjId  --16
  ,SvcProvShortName = cdv.SvcProvShortName  --17
  ,DivHpoObjId = cdv.DivHpoObjId  --18
  ,DivShortName = cdv.DivShortName  --19
  ,BusUnitHpoObjId = cdv.BusUnitHpoObjId  --20
  ,BusUnitShortName = cdv.BusUnitShortName  --21

  ,ChgProcCdVal = cdv.ChgProcCdVal  --22
  ,ChgProcCdDesc = cdv.ChgProcCdDesc  --23
  ,ChgModfVal = cdv.ChgModfVal  --24

  ,BaseWorkRvu = chg.work_rvu  --25
  ,TtlAdjWorkRvu = cdv.TtlAdjWorkRvu  --26
  ,Qty = cdv.Qty  --27
  ,ExtPriceAmt = cdv.ExtPriceAmt  --28

  ,PrimPyrObjId = cdv.PrimPyrObjId  --29
  ,PrimPyrMne = cdv.PrimPyrMne  --30
  ,PrimPyrShortName = cdv.PrimPyrShortName  --31

  ,CurrPyrObjId = cdv.CurrPyrObjId  --32
  ,CurrPyrMne = cdv.CurrPyrMne  --33
  ,CurrPyrShortName = cdv.CurrPyrShortName  --34

  ,CostCenter = gl.cost_center  --35
  ,FundId = gl.fund_id  --36
  ,SAPGL = gl.sap_gl  --37
  ,SvcSysGenId = cdv.SvcSysGenId  --38

  ,PosCd = cdv.PosCd  --39
  ,PosDesc = cdv.PosDesc  --40
  ,SupvHlthProfTypeMne = SupHlthProf.TypeMne  --41
  ,SupvHlthProvHQPMNo = SupvNPIDtl.HlthProfPriHQPMNo  --42
  ,PerfHlthProfObjId = cdv.PerfHlthProfObjId  --43
  ,PerfHlthProfName = PerfNPIDtl.HlthProfName  --44
  ,PerfHlthProfNPI = PerfNPIDtl.HlthProfNPI  --45
  ,PerfHlthProfHQPM = PerfNPIDtl.HlthProfPriHQPMNo  --46
  ,PerfHlthProfTypeMne = SupHlthProf.TypeMne  --47
  ,ELShortName = cdv.ELShortName  --48
  ,AnesPhysUnits = cdv.AnesPhysUnits  --48
  ,AnesProcDurMins = cdv.AnesProcDurMins  --50
  ,AnesProcBaseUnits = cdv.AnesProcBaseUnits  --51
  ,AnesStartDtime = cdv.AnesStartDtime  --52
  ,AnesStopDtime = cdv.AnesStopDtime  --53
  ,AnesSurgProcCd = cdv.AnesSurgProcCd  --54
  ,AnesTimeUnits = cdv.AnesTimeUnits  --55
  ,AnesTotUnits = cdv.AnesTotUnits  --56
  ,AnesModVal1 = cdv.AnesModVal1  --57
  ,AnesModVal2 = cdv.AnesModVal2  --58
  ,AnesModVal3 = cdv.AnesModVal3  --59
  ,AnesModVal4 = cdv.AnesModVal4  --60
  ,AnesModVal5 = cdv.AnesModVal5  --61

  ,RvsePostDate = cdv.RvsePostDate
  ,TransTypeMne = cdv.TransTypeMne

INTO Staging.UKFinSfVolumeMonthly	

FROM KMSFDSSDBP01.smsphdssf240.customer.kmsfsfchgdtlv cdv
  JOIN KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sf_chg chg ON chg.GlEntObjId = cdv.GlEntObjId
  JOIN KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sf_acct_pd_date apd ON CAST(cdv.RptPostBusDate AS DATE) = CAST(apd.FullDate AS DATE)
  LEFT JOIN KMSFDSSDBP01.smsphdssf240.smsdss.HlthProfMstrV SupHlthProf ON SupHlthProf.HlthProfObjId = cdv.SupvHlthProfObjId
  LEFT JOIN KMSFDSSDBP01.smsphdssf240.smsdss.HlthProfMstrV PerfHlthProf ON PerfHlthProf.HlthProfObjId = cdv.PerfHlthProfObjId
  LEFT JOIN KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sap_gl_assn gl ON gl.chg_type = 1 AND gl.GLEntObjId = cdv.GLEntObjId AND gl.current_record = 1
  LEFT JOIN KMSFDSSDBP01.smsphdssf240.Customer.KmsfHlthProfNPIDtl SupvNPIDtl ON SupvNPIDtl.HlthProfNPI = cdv.SupvHlthProfNPI
  LEFT JOIN KMSFDSSDBP01.smsphdssf240.Customer.KmsfHlthProfNPIDtl PerfNPIDtl ON PerfNPIDtl.HlthProfNPI = cdv.PerfHlthProfNPI

WHERE cdv.RptPostBusDate BETWEEN @VMFsclStartDate AND @VMFsclStopDate

--DECLARE @ExportName VARCHAR(254) = '\\kmsfweb3\snapshots\BusinessOffice\Budget\SF Volume Monthly 20170925 Sample Data.txt'
--EXEC zsmsphdssf240z.dbo.kmsf_write_file_sp 
--  @command = 'select  * from KmsfObExtracts.Staging.UKFinSfVolumeMonthly',
--  @type = 'text',
--  @filePath = @ExportName, 
--  @columnHeaders = 'Y',
--  @delimeter = '|'

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl'

SELECT @RsltCd = '1'

RETURN 0

END












GO


