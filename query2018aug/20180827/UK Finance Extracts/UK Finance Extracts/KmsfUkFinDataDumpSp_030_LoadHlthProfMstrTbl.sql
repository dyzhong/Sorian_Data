USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_030_LoadHlthProfMstrTbl]    Script Date: 8/8/2018 8:19:19 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_030_LoadHlthProfMstrTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_030_LoadHlthProfMstrTbl','')

SET NOCOUNT ON

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

IF OBJECT_ID('Staging.UKFinHlthProfMstr','U') is not null
	DROP TABLE Staging.UKFinHlthProfMstr
SELECT HlthProfObjId = hpms.HlthProfObjId
  ,HlthProfNPI = hpms.HlthProfNPI
  ,DegreeText = hpms.DegreeText
  ,FirstName = hpms.GivenName
  ,MiddleName = hpms.MiddleName
  ,LastName = hpms.FamName
  ,SpcltyCdVal = NPIDtl.HlthProfPriTxnmyCd
  ,SpcltyCdMne = TxnmyDesc.Mne
  ,SpcltyCdDesc = TxnmyDesc.DescText
  ,HlthProfTypeMne = hpms.TypeMne
INTO Staging.UKFinHlthProfMstr
FROM KMSFDSSDBP01.smsphdssf240.smsdss.HlthProfMstrV hpms
  JOIN KMSFDSSDBP01.smsphdssf240.[Customer].[KmsfHlthProfNPIDtl] NPIDtl ON NPIDtl.HlthProfNPI = hpms.HlthProfNPI
  LEFT JOIN KMSFDSSDBP01.smsphdssf240.smsdss.CdValues_SPECIALTY TxnmyDesc ON TxnmyDesc.CrossRefCd = NPIDtl.HlthProfPriTxnmyCd

--DECLARE @ExportName VARCHAR(254) = '\\kmsfweb3\snapshots\BusinessOffice\Budget\Provider Master 20170925 Sample Data.txt'
--EXEC zsmsphdssf240z.dbo.kmsf_write_file_sp 
--  @command = 'select  * from KmsfObExtracts.Staging.UKFinHlthProfMstr',
--  @type = 'text',
--  @filePath = @ExportName, 
--  @columnHeaders = 'Y',
--  @delimeter = '|'

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_030_LoadHlthProfMstrTbl'

SELECT @RsltCd = '1'

RETURN 0

END








GO


