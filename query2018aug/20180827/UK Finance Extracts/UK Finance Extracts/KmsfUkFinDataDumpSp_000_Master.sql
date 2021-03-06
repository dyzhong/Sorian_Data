USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_000_Master]    Script Date: 8/8/2018 8:18:34 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_000_Master]
  (
     @ReProcOpt VARCHAR(1) = NULL  --1=True, 0=False
    ,@FsclYrPer VARCHAR(6) = NULL
    ,@FsclStartDate DATE = NULL
    ,@FsclStopDate DATE = NULL
    ,@VMFsclStartDate DATE = NULL
    ,@VMFsclStopDate DATE = NULL
    ,@ExportPath VARCHAR(254) = NULL
    ,@ExportName VARCHAR(254) = NULL
    ,@StartDtime DATETIME2(7) = NULL
    ,@EndDtime DATETIME2(7) = NULL
    ,@LogEntryId BIGINT = NULL
    ,@RsltCd tinyint = NULL
    ,@RsltTxt varchar(254) = NULL
  )

AS

BEGIN

SET NOCOUNT ON

--DECLARE @ReProcOpt VARCHAR(1) = NULL  --1=True, 0=False
--  ,@FsclYrPer VARCHAR(6) = NULL
--  ,@StartDtime DATETIME2(7) = NULL
--  ,@EndDtime DATETIME2(7) = NULL
--  ,@LogEntryId BIGINT = NULL

SET @StartDtime = SYSDATETIME()

SET @ReProcOpt = ISNULL(@ReProcOpt,0) --Default To False

IF @ReProcOpt = 0
  BEGIN

    SELECT @FsclYrPer = ((SELECT MIN(FsclYrPer) 
                         FROM KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sf_acct_pd
                         WHERE FsclYrPer > ISNULL((SELECT MAX(FsclYrPer) FROM kmsf.KmsfUkFinDataDumpProcLog WHERE ISNULL(RsltCd,0) = 1),'201605')
                           AND DATEADD("D",5,CAST(FsclPerEnd AS DATE)) < GETDATE()
                           ))

    IF @FsclYrPer IS NULL
      BEGIN
        RETURN 0
      END
                             
  END

DELETE FROM kmsf.KmsfUkFinDataDumpProcLogDtl WHERE LogEntryId IN (SELECT LogEntryId FROM kmsf.KmsfUkFinDataDumpProcLog WHERE FsclYrPer = @FsclYrPer)
DELETE FROM kmsf.KmsfUkFinDataDumpProcLog WHERE FsclYrPer = @FsclYrPer

INSERT INTO kmsf.KmsfUkFinDataDumpProcLog (ReProcOpt,FsclYrPer,StartDtime,RsltCd) VALUES (@ReProcOpt,@FsclYrPer,@StartDtime,0)
SET @LogEntryId = @@IDENTITY

IF ISNULL(@FsclYrPer,'') = ''
      BEGIN
        UPDATE kmsf.KmsfUkFinDataDumpProcLog SET RsltTxt = 'Process Failed - No Fiscal Period Was Passed' WHERE LogEntryId = @LogEntryId
        RETURN 0
        --SELECT RsltTxt = 'Process Failed - No Fiscal Period Was Passed'
      END

--IF @FsclYrPer > (SELECT MAX(FsclYrPer) FROM KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sf_acct_pd WHERE DATEADD("D",5,CAST(FsclPerEnd AS DATE)) < GETDATE())
--  BEGIN
--    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET RsltTxt = 'Process Failed - The Maximum Fiscal Year Period Was Violated' WHERE LogEntryId = @LogEntryId
--    RETURN 0
--    --SELECT RsltTxt = 'Process Failed - The Minimum Fiscal Year Period Was Violated'
--  END

IF @FsclYrPer < '201512'
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET RsltTxt = 'Process Failed - The Minimum Fiscal Year Period Was Violated' WHERE LogEntryId = @LogEntryId
    RETURN 0
    --SELECT RsltTxt = 'Process Failed - The Minimum Fiscal Year Period Was Violated'
  END

SET @FsclStartDate = (SELECT FsclPerStart FROM KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_sf_acct_pd WHERE FsclYrPer = @FsclYrPer)
SET @FsclStopDate = (SELECT FsclPerEnd FROM KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_sf_acct_pd WHERE FsclYrPer = @FsclYrPer)

SET @VMFsclStartDate = (SELECT FsclPerStart FROM KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_sf_acct_pd WHERE FsclYrPer = @FsclYrPer)
SET @VMFsclStartDate = DATEADD(m,-5,@VMFsclStartDate)
SET @VMFsclStartDate = CASE WHEN @VMFsclStartDate < '2015-12-01' THEN '2015-12-01' ELSE @VMFsclStartDate END 
SET @VMFsclStopDate = (SELECT FsclPerEnd FROM KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_sf_acct_pd WHERE FsclYrPer = @FsclYrPer)

SET @ExportPath = '\\kmsfweb3\snapshots\BusinessOffice\Budget\'

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl @LogEntryId = @LogEntryId, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl @LogEntryId = @LogEntryId, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_030_LoadHlthProfMstrTbl @LogEntryId = @LogEntryId, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_040_LoadHlthProfSpcltyTbl @LogEntryId = @LogEntryId, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_050_LoadSfBudgetGCRTbl @LogEntryId = @LogEntryId, @FsclStartDate = @FsclStartDate, @FsclStopDate = @FsclStopDate, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_060_LoadSfVolumeMonthlyTbl @LogEntryId = @LogEntryId, @VMFsclStartDate = @VMFsclStartDate, @VMFsclStopDate = @VMFsclStopDate, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl @LogEntryId = @LogEntryId, @FsclYrPer = @FsclYrPer, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

SELECT @RsltCd = NULL, @RsltTxt = NULL 
EXEC kmsf.KmsfUkFinDataDumpSp_090_ExportDataFiles @LogEntryId = @LogEntryId, @FsclStartDate = @FsclStartDate, @FsclStopDate = @FsclStopDate, @VMFsclStartDate = @VMFsclStartDate, @VMFsclStopDate = @VMFsclStopDate, @ExportPath = @ExportPath, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

IF ISNULL(@RsltCd,0) <> 1
  BEGIN
    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
    RETURN 0
  END

--SELECT @RsltCd = NULL, @RsltTxt = NULL 
--EXEC kmsf.KmsfUkFinDataDumpSp_999_FinalCleanup @LogEntryId = @LogEntryId, @RsltCd = @RsltCd OUTPUT, @RsltTxt = @RsltTxt OUTPUT

--IF ISNULL(@RsltCd,0) <> 1
--  BEGIN
--    UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = @RsltCd, RsltTxt = @RsltTxt WHERE LogEntryId = @LogEntryId
--    RETURN 0
--  END

UPDATE kmsf.KmsfUkFinDataDumpProcLog SET EndDtime = SYSDATETIME(), RsltCd = 1, RsltTxt = 'Success' WHERE LogEntryId = @LogEntryId

RETURN 0

SET NOCOUNT OFF

END






GO


