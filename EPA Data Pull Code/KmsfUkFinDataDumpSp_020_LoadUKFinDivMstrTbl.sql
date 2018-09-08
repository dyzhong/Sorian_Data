USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl]    Script Date: 11/30/2017 9:51:28 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl','')

SET NOCOUNT ON

IF OBJECT_ID('Staging.UKFinDivMstr','U') is not null
	DROP TABLE Staging.UKFinDivMstr
SELECT DivHpoObjId = HPOObjId
  ,DivShortName = ShortName
INTO Staging.UKFinDivMstr
FROM KMSFDSSDBP01.SMSPHDSSF240.smsdss.hpomstrv
WHERE TypeMne = 'Division'

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_020_LoadUKFinDivMstrTbl'

SELECT @RsltCd = '1'

RETURN 0

END







GO


