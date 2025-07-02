-------------------------------------------------------------------------------------------------------------------
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.SUPVERISION_ALL_CASES;
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.CasesToConvert;
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.ChargesToConvert;
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.PartiesToConvert;
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.WarrantsToConvert;
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.HearingsToConvert;
	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.EventsToConvert;
-------------------------------------------------------------------------------------------------------------------
	SELECT DISTINCT
		  S.CaseID AS SUPERVISION_CASEID
		 ,CAHSUP.CaseNbr AS SUPERVISION_CASENUMBER
		 ,CAST(SUPCS.CaseUTypeID AS VARCHAR) + '-' + UC1.Code + ':' + UC1.[Description] AS SUPERVISION_CASETYPE
		 ,CAHCRT.CaseID AS COURTCASE_CASEID
		 ,CAHCRT.CaseNbr AS COURTCASE_CASENUMBER
		 ,CAST(CRTCS.CaseUTypeID AS VARCHAR) + '-' + UC2.Code + ':' + UC2.[Description] AS COURTCASE_CASETYPE
		 ,XCB.CaseID AS SUPCASEID_XCASEBASECHRG
		 ,XCB.ChargeID 
		 ,XCB2.CaseID AS CLKCASEID_XCASEBASECHRG
	INTO {{DB_NAME}}.DBO.SUPVERISION_ALL_CASES
	FROM
		Justice.DBO.SUPCASEHDR S WITH (NOLOCK)
			INNER JOIN Justice.DBO.xCaseBaseChrg XCB WITH (NOLOCK) ON S.CaseID=XCB.CaseID
			INNER JOIN Justice.DBO.CLKCASEHDR SUPCS WITH (NOLOCK) ON XCB.CaseID=SUPCS.CaseID
			INNER JOIN Justice.DBO.CaseAssignHist CAHSUP WITH (NOLOCK) ON SUPCS.CaseAssignmentHistoryIDCur=CAHSUP.CaseAssignmentHistoryID
			INNER JOIN Justice.DBO.UCODE UC1 WITH (NOLOCK) ON SUPCS.CaseUTypeID=UC1.CodeID
			LEFT JOIN Justice.DBO.xCaseBaseChrg XCB2 WITH (NOLOCK) ON XCB.ChargeID=XCB2.ChargeID
			LEFT JOIN Justice.DBO.CLKCASEHDR CRTCS WITH (NOLOCK) ON XCB2.CaseID=CRTCS.CaseID AND SUPCS.CaseID<>XCB2.CaseID
			LEFT JOIN Justice.DBO.CaseAssignHist CAHCRT WITH (NOLOCK) ON CRTCS.CaseAssignmentHistoryIDCur=CAHCRT.CaseAssignmentHistoryID
			LEFT JOIN Justice.DBO.UCODE UC2 WITH (NOLOCK) ON CRTCS.CaseUTypeID=UC2.CodeID
	ORDER BY 
		S.CaseID,CAHCRT.CaseID;
-------------------------------------------------------------------------------------------------------------------
		SELECT SUPERVISION_CASEID AS CaseID,'SUPCASEHDR' AS TYPEOFCASE INTO {{DB_NAME}}.DBO.CasesToConvert FROM {{DB_NAME}}.DBO.SUPVERISION_ALL_CASES GROUP BY SUPERVISION_CASEID
	UNION
		SELECT COURTCASE_CASEID AS CaseID,'CLKCASEHDR' AS TYPEOFCASE FROM {{DB_NAME}}.DBO.SUPVERISION_ALL_CASES WHERE COURTCASE_CASEID NOT IN (SELECT DISTINCT SUPERVISION_CASEID FROM {{DB_NAME}}.DBO.SUPVERISION_ALL_CASES) GROUP BY COURTCASE_CASEID;
-------------------------------------------------------------------------------------------------------------------
	ALTER TABLE {{DB_NAME}}.DBO.CasesToConvert ALTER COLUMN CaseID INT NOT NULL;
	ALTER TABLE {{DB_NAME}}.DBO.CasesToConvert ADD CONSTRAINT PK_CaseID PRIMARY KEY (CaseID);
	-------------------------------------------------------------------------------------------------------------------
