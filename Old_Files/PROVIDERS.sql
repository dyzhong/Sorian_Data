TRUNCATE TABLE DEV_EXP_TGT.EPAT.PROVIDERS;\

INSERT INTO DEV_EXP_TGT.EPAT.PROVIDERS SELECT  * ,'0' AS EXP_CHK_SUM_NUMBER from DEV_EXP_STG.EPAT.PROVIDERS_STG ;\

