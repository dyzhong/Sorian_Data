USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_040_LoadHlthProfSpcltyTbl]    Script Date: 11/30/2017 9:51:58 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_040_LoadHlthProfSpcltyTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_040_LoadHlthProfSpcltyTbl','')

SET NOCOUNT ON

IF OBJECT_ID('Staging.UKFinHlthProfSpclty','U') IS NOT NULL
	DROP TABLE Staging.UKFinHlthProfSpclty
SELECT HlthProfObjId = hps.HlthProfObjId
  ,SpcltyCdVal = spclty.CrossRefCd
  ,SpcltyCdMne = spclty.Mne
  ,SpcltyCdDesc = spclty.DescText
INTO Staging.UKFinHlthProfSpclty
FROM KMSFDSSDBP01.SMSPHDSSF240.dbo.HlthProfSpcl hps
  JOIN KMSFDSSDBP01.SMSPHDSSF240.smsdss.CdValues_SPECIALTY AS spclty ON hps.SpclCd = spclty.IntrnCd

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_040_LoadHlthProfSpcltyTbl'

SELECT @RsltCd = '1'

RETURN 0

END









GO


