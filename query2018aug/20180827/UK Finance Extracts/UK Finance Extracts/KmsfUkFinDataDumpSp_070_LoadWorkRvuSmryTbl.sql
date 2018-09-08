USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl]    Script Date: 8/8/2018 8:20:05 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO









CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@FsclYrPer VARCHAR(6) = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl','')

SET NOCOUNT ON

if object_id('Staging.UKFinWorkRvuDtl','U') is not null
  drop table Staging.UKFinWorkRvuDtl
create table Staging.UKFinWorkRvuDtl
  (
	  [id_col] [int] identity (1,1) not null,
	  [FsclYr] [int] null,
	  [FsclPer] [tinyint] null,
	  [FsclYrPer] [int] null,
	  [BusUnitShortName] [varchar](254) null,
	  [DivShortName] [varchar](254) null,
	  [TotAdjWorkRvu] [decimal](12, 2) null,
    [RowSrce] [varchar](254) null
  )

if object_id('Staging.UKFinWorkRvuSFDiv','U') is not null
  drop table Staging.UKFinWorkRvuSFDiv
select distinct DivHpoObjId = [Div HpoObjId]
  ,DivShortName = [Div Short Name]
into Staging.UKFinWorkRvuSFDiv
from KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_sf_sp_hier_v

insert into Staging.UKFinWorkRvuDtl
  (
     [FsclYr]
    ,[FsclPer]
    ,[FsclYrPer]
    ,[BusUnitShortName]
    ,[DivShortName]
    ,[TotAdjWorkRvu]
    ,[RowSrce]
  )
select [FlscYr] = fs.acct_yr
  ,[FsclPer] = fs.acct_pd
  ,[FsclYrPer] = fs.acct_yr_pd
  ,[BusUnitShortName] = COALESCE(Div.DeptHPOShortName,Dept.DeptHPOShortName,'Sig-' + h.lvl4_name)
  ,lvl5_id = COALESCE(Div.DivHPOShortName,'Sig-' + h.lvl5_name)
  ,[TotAdjWorkRvu] = sum(fs.work_rvu)
  ,[RowSrce] = 'Sig'
from KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_fin_summ fs
  left join KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_rpt_hier h on h.prov_id = fs.orgz_cd
  LEFT JOIN KMSFDSSDBP01.SMSPHDSSF240.Customer.KmsfSfSigDeptToSfBusUnitXWalk Dept	ON fs.lvl4_id = Dept.lvl4_id
  LEFT JOIN KMSFDSSDBP01.SMSPHDSSF240.Customer.KmsfSfSigDivToSfBusUnitDivXWalk Div ON fs.lvl5_id = Div.lvl5_id
where fs.acct_yr_pd = @FsclYrPer
  and fs.work_rvu is not null
group by fs.acct_yr
  ,fs.acct_pd
  ,fs.acct_yr_pd
  ,COALESCE(Div.DeptHPOShortName,Dept.DeptHPOShortName,'Sig-' + h.lvl4_name)
  ,COALESCE(Div.DivHPOShortName,'Sig-' + h.lvl5_name)
having sum(fs.work_rvu) <> 0

insert into Staging.UKFinWorkRvuDtl
  (
     [FsclYr]
    ,[FsclPer]
    ,[FsclYrPer]
    ,[BusUnitShortName]
    ,[DivShortName]
    ,[TotAdjWorkRvu]
    ,[RowSrce]
  )
SELECT [FlscYr] = CYTD.acct_yr
  ,[FsclPer] = CYTD.acct_pd
  ,[FsclYrPer] = CYTD.acct_yr_pd
  ,[BusUnitShortName] = CYTD.[Department]
  ,[DivShortName] = CYTD.[Division]
  ,[TotAdjWorkRvu] = sum(isnull(CYTD.work_rvu, 0))
  ,[RowSrce] = 'SF'
FROM KMSFDSSDBP01.SMSPHDSSF240.Customer.Charges_YTD CYTD
  LEFT JOIN KMSFDSSDBP01.SMSPHDSSF240.smsdss.kmsf_acct_pd_desc pd	ON CYTD.acct_pd = right('00'+cast(pd.acct_pd as varchar(2)),2)
  LEFT JOIN Staging.UKFinWorkRvuSFDiv d1 ON d1.DivShortName = CYTD.Division
WHERE CYTD.acct_yr_pd = @FsclYrPer
  AND isnull(CYTD.work_rvu,0) <> 0
GROUP BY CYTD.acct_yr
  ,CYTD.acct_pd
  ,CYTD.acct_yr_pd
  ,CYTD.[Department]
  ,CYTD.[Division]
HAVING sum(isnull(CYTD.work_rvu, 0)) <> 0

if object_id('Staging.UKFinWorkRvuSmry','U') is not null
	drop table Staging.UKFinWorkRvuSmry
SELECT [FsclYr]
  ,[FsclPer]
  ,[FsclYrPer]
  ,[BusUnitShortName]
  ,[DivShortName]
  ,[TotAdjWorkRvu] = SUM([TotAdjWorkRvu])
INTO Staging.UKFinWorkRvuSmry
FROM Staging.UKFinWorkRvuDtl
GROUP BY [FsclYr]
  ,[FsclPer]
  ,[FsclYrPer]
  ,[BusUnitShortName]
  ,[DivShortName]
HAVING SUM([TotAdjWorkRvu]) <> 0

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_070_LoadWorkRvuSmryTbl'

SELECT @RsltCd = '1'

RETURN 0

END












GO


