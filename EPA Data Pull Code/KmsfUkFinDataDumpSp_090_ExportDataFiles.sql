USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_090_ExportDataFiles]    Script Date: 11/30/2017 9:52:58 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_090_ExportDataFiles]
  (
     @LogEntryId BIGINT = NULL
    ,@FsclStartDate DATE = NULL
    ,@FsclStopDate DATE = NULL
    ,@VMFsclStartDate DATE = NULL
    ,@VMFsclStopDate DATE = NULL
    ,@ExportPath VARCHAR(254) = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

 INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
 VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_090_ExportDataFiles','')

SET NOCOUNT ON

DECLARE @ExportName VARCHAR(254)

SET @ExportName = @ExportPath + 'BusUnitMstr ' + REPLACE(CAST(CAST(@FsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@FsclStopDate AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
  @command = 'select * from KmsfObExtracts.Staging.UKFinBusUnitMstr',
  @type = 'text',
  @filePath = @ExportName, 
  @columnHeaders = 'Y',
  @delimeter = '|'

SET @ExportName = @ExportPath + 'DivMstr ' + REPLACE(CAST(CAST(@FsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@FsclStopDate AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
  @command = 'select * from KmsfObExtracts.Staging.UKFinDivMstr',
  @type = 'text',
  @filePath = @ExportName, 
  @columnHeaders = 'Y',
  @delimeter = '|'

SET @ExportName = @ExportPath + 'SF Hlth Prof Mstr ' + REPLACE(CAST(CAST(@FsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@FsclStopDate AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
  @command = 'select * from KmsfObExtracts.Staging.UKFinHlthProfMstr',
  @type = 'text',
  @filePath = @ExportName, 
  @columnHeaders = 'Y',
  @delimeter = '|'

SET @ExportName = @ExportPath + 'SF Hlth Prof Spclty ' + REPLACE(CAST(CAST(@FsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@FsclStopDate AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
  @command = 'select * from KmsfObExtracts.Staging.UKFinHlthProfSpclty',
  @type = 'text',
  @filePath = @ExportName, 
  @columnHeaders = 'Y',
  @delimeter = '|'

SET @ExportName = @ExportPath + 'SF Budget GCR ' + REPLACE(CAST(CAST(@FsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@FsclStopDate AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
    @command = 'select * from KmsfObExtracts.Staging.UKFinSfBudgetGCR',
     @type = 'text',
     @filePath = @ExportName, 
     @columnHeaders = 'Y',
     @delimeter = '|'

SET @ExportName = @ExportPath + 'SF Volume Monthly ' + REPLACE(CAST(CAST(@VMFsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@VMFsclStopDate AS DATE) AS VARCHAR(10)),'-','') + ' Run Date ' + REPLACE(CAST(CAST(SYSDATETIME() AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
  @command = 'select  * from KmsfObExtracts.Staging.UKFinSfVolumeMonthly',
  @type = 'text',
  @filePath = @ExportName, 
  @columnHeaders = 'Y',
  @delimeter = '|'

SET @ExportName = @ExportPath + 'Work Rvu Summary ' + REPLACE(CAST(CAST(@FsclStartDate AS DATE) AS VARCHAR(10)),'-','') + ' - ' + REPLACE(CAST(CAST(@FsclStopDate AS DATE) AS VARCHAR(10)),'-','') + '.txt'
EXEC dbo.kmsf_write_file_sp 
  @command = 'select  * from KmsfObExtracts.Staging.UKFinWorkRvuSmry',
  @type = 'text',
  @filePath = @ExportName, 
  @columnHeaders = 'Y',
  @delimeter = '|'

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_090_ExportDataFiles'

SELECT @RsltCd = '1'

RETURN 0

END





GO


