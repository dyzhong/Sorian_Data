USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl]    Script Date: 11/30/2017 9:51:12 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl','')

SET NOCOUNT ON

IF OBJECT_ID('Staging.UKFinBusUnitMstr','U') is not null
	DROP TABLE Staging.UKFinBusUnitMstr
SELECT BusUnitHpoObjId = HPOObjId
  ,BusUnitShortName = ShortName
INTO Staging.UKFinBusUnitMstr
FROM KMSFDSSDBP01.SMSPHDSSF240.smsdss.hpomstrv
WHERE TypeMne = 'Bus Unit'

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_010_LoadUKFinBusUnitMstrTbl'

SELECT @RsltCd = '1'

RETURN 0

END






GO


