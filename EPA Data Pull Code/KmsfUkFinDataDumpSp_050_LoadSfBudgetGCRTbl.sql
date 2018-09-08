USE [KmsfOBExtracts]
GO

/****** Object:  StoredProcedure [kmsf].[KmsfUkFinDataDumpSp_050_LoadSfBudgetGCRTbl]    Script Date: 11/30/2017 9:52:12 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







CREATE PROCEDURE [kmsf].[KmsfUkFinDataDumpSp_050_LoadSfBudgetGCRTbl]
  (
     @LogEntryId BIGINT = NULL
    ,@FsclStartDate DATE = NULL
    ,@FsclStopDate DATE = NULL
    ,@RsltCd TINYINT OUTPUT
    ,@RsltTxt VARCHAR(254) OUTPUT
  )

AS

BEGIN

INSERT INTO kmsf.KmsfUkFinDataDumpProcLogDtl (LogEntryId,StartDtime,ProcName,ValueVariable)
VALUES (@LogEntryId,SYSDATETIME(),'KmsfUkFinDataDumpSp_050_LoadSfBudgetGCRTbl','')

SET NOCOUNT ON

IF OBJECT_ID('Staging.UKFinSfBudgetGCR','U') IS NOT NULL
	DROP TABLE Staging.UKFinSfBudgetGCR
SELECT

   EncMRN = CASE WHEN gl.chg_type = 1 THEN cdv.EncMRN ELSE cdv2.EncMrn END
  ,EcdNo = CASE WHEN gl.chg_type = 1 THEN cdv.EcdNo ELSE cdv2.EcdNo END
  ,EncTypeMne = CASE WHEN gl.chg_type = 1 THEN cdv.EncTypeMne ELSE cdv2.EncTypeMne END

  ,EncLocId = CASE WHEN gl.chg_type = 1 THEN cdv.EncLocId ELSE cdv2.EncLocId END
  ,ApmSchedLocCd = CASE WHEN gl.chg_type = 1 THEN cdv.ApmSchedLocCd ELSE cdv2.ApmSchedLocCd END

  ,ChgObjId = CASE WHEN gl.chg_type = 1 THEN cdv.ChgObjId ELSE cdv2.ChgObjId END
  ,ChgGlEntObjId = CASE WHEN gl.chg_type = 1 THEN cdv.GlEntObjId ELSE cdv2.GlEntObjId END
  ,FinGLEntObjId = CASE WHEN gl.chg_type = 2 THEN fin.GlEntObjId ELSE NULL END

  ,StrDate = CASE WHEN gl.chg_type = 1 THEN CAST(cdv.StrDate AS DATE) ELSE CAST(cdv2.StrDate AS DATE) END  

  ,FsclYr = pd.FsclYr
  ,FlscPer = pd.FsclPer
  ,FsclYrPer = pd.FsclYrPer

  ,SupvHlthProfObjId = CASE WHEN gl.chg_type = 1 THEN cdv.SupvHlthProfObjId ELSE cdv2.SupvHlthProfObjId END
  ,SupvHlthProfNpi = CASE WHEN gl.chg_type = 1 THEN cdv.SupvHlthProfNpi ELSE cdv2.SupvHlthProfNpi END
  ,SupvHlthProfName = CASE WHEN gl.chg_type = 1 THEN cdv.SupvHlthProfName ELSE cdv2.SupvHlthProfName END

  ,SvcProvHPOObjId = CASE WHEN gl.chg_type = 1 THEN cdv.SvcProvHPOObjId ELSE cdv2.SvcProvHPOObjId END
  ,SvcProvShortName = CASE WHEN gl.chg_type = 1 THEN cdv.SvcProvShortName ELSE cdv2.SvcProvShortName END
  ,DivHpoObjId = CASE WHEN gl.chg_type = 1 THEN cdv.DivHpoObjId ELSE cdv2.DivHpoObjId END
  ,DivShortName = CASE WHEN gl.chg_type = 1 THEN cdv.DivShortName ELSE cdv2.DivShortName END
  ,BusUnitHpoObjId = CASE WHEN gl.chg_type = 1 THEN cdv.BusUnitHpoObjId ELSE cdv2.BusUnitHpoObjId END
  ,BusUnitShortName = CASE WHEN gl.chg_type = 1 THEN cdv.BusUnitShortName ELSE cdv2.BusUnitShortName END

  ,ChgProcCdVal = CASE WHEN gl.chg_type = 1 THEN cdv.ChgProcCdVal ELSE cdv2.ChgProcCdVal END
  ,ChgProcCdDesc = CASE WHEN gl.chg_type = 1 THEN cdv.ChgProcCdDesc ELSE cdv2.ChgProcCdDesc END
  ,SvcObjId = CASE WHEN gl.chg_type = 1 THEN cdv.SvcObjId ELSE cdv2.SvcObjId END
  ,SvcBlName = CASE WHEN gl.chg_type = 1 THEN cdv.SvcBlName ELSE cdv2.SvcBlName END
    
  ,ServiceGroup1 = CASE WHEN gl.chg_type = 1 THEN svc.RptGrp1Mne ELSE svc2.RptGrp1Mne END
  ,ServiceGroup2 = CASE WHEN gl.chg_type = 1 THEN svc.RptGrp2Mne ELSE svc2.RptGrp2Mne END

  ,GL = gl.sap_gl 
  ,GLDesc = glDesc.sap_gl_Desc
  ,ChgAmt = CASE WHEN gl.chg_type = 1 THEN amount ELSE 0 END
  ,PmtAmt = CASE WHEN gl.chg_type = 2 THEN amount * -1 ELSE 0 END
  ,TransType = CASE WHEN gl.chg_type = 1 THEN 'Charge' ELSE 'Payment' END

  ,PyrObjId = NULL
  ,PyrMne = NULL
  ,PyrShortName = NULL

into Staging.UKFinSfBudgetGCR
from KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sap_gl_assn gl
  join KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sap_gl glDesc on gl.sap_gl = glDesc.sap_gl
  left join KMSFDSSDBP01.smsphdssf240.smsdss.kmsf_sf_acct_pd_date pd on CAST(gl.sf_work_date AS DATE) = CAST(pd.fullDate AS DATE)
   
  left join KMSFDSSDBP01.smsphdssf240.smsdss.FinTransFctV fin on gl.chg_type = 2 and gl.GLEntObjId = fin.GLEntObjId 
  left join KMSFDSSDBP01.smsphdssf240.smsdss.ChgFctV chg on gl.chg_type = 1 and gl.GLEntObjId = chg.GLEntObjId 
  left join KMSFDSSDBP01.smsphdssf240.smsdss.ChgFctV chg2 on gl.chg_type = 2 and fin.ChgObjId = chg2.ChgObjId and chg2.TransTypeCd = 1
   
  left join KMSFDSSDBP01.smsphdssf240.smsdss.SvcMstrV svc on gl.chg_type = 1 and chg.SvcObjId = svc.SvcObjId
  left join KMSFDSSDBP01.smsphdssf240.smsdss.SvcMstrV svc2 on gl.chg_type = 2 and chg2.SvcObjId = svc2.SvcObjId

  left join KMSFDSSDBP01.smsphdssf240.Customer.KmsfSfChgDtlV cdv ON gl.chg_type = 1 AND chg.GlEntObjId = cdv.GlEntObjId
  left join KMSFDSSDBP01.smsphdssf240.Customer.KmsfSfChgDtlV cdv2 ON gl.chg_type = 2 AND chg2.GlEntObjId = cdv2.GlEntObjId

where sf_work_date between @FsclStartDate and @FsclStopDate
  and processed_ind = 1

UPDATE kmsf.KmsfUkFinDataDumpProcLogDtl SET EndDtime = SYSDATETIME() WHERE LogEntryId = @LogEntryId AND ProcName = 'KmsfUkFinDataDumpSp_050_LoadSfBudgetGCRTbl'

SELECT @RsltCd = '1'

RETURN 0

END










GO


