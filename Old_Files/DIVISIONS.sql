TRUNCATE TABLE DEV_EXP_TGT.EPAT.DIVISIONS;\

INSERT INTO DEV_EXP_TGT.EPAT.DIVISIONS SELECT  * ,'0' AS EXP_CHK_SUM_NUMBER from DEV_EXP_STG.EPAT.DIVISIONS_STG ;\

