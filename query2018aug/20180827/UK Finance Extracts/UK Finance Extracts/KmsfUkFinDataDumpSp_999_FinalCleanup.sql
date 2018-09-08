USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_999_FinalCleanup]    Script Date: 8/8/2018 8:20:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_999_FinalCleanup]
  (
     @LogEntryId BIGINT = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

 INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
 VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_999_FinalCleanup','')

SET NOCOUNT ON

IF OBJECT_ID('Staging.UKFinBusUnitMstr','U') IS NOT NULL
  DROP TABLE Staging.UKFinBusUnitMstr

IF OBJECT_ID('Staging.UKFinDivMstr','U') IS NOT NULL
  DROP TABLE Staging.UKFinDivMstr

IF OBJECT_ID('Staging.UKFinHlthProfMstr','U') IS NOT NULL
  DROP TABLE Staging.UKFinHlthProfMstr

IF OBJECT_ID('Staging.UKFinHlthProfSpclty','U') IS NOT NULL
  DROP TABLE Staging.UKFinHlthProfSpclty

IF OBJECT_ID('Staging.UKFinSfBudgetGCR','U') IS NOT NULL
  DROP TABLE Staging.UKFinSfBudgetGCR

IF OBJECT_ID('Staging.UKFinSfVolumeMonthly','U') IS NOT NULL
  DROP TABLE Staging.UKFinSfVolumeMonthly

IF OBJECT_ID('Staging.UKFinWorkRvuDtl','U') IS NOT NULL
  DROP TABLE Staging.UKFinWorkRvuDtl

IF OBJECT_ID('Staging.UKFinWorkRvuSFDiv','U') IS NOT NULL
  DROP TABLE Staging.UKFinWorkRvuSFDiv

IF OBJECT_ID('Staging.UKFinWorkRvuSmry','U') IS NOT NULL
  DROP TABLE Staging.UKFinWorkRvuSmry

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_999_FinalCleanup'

SELECT @RsltCd = '1'

RETURN 0

END






GO


