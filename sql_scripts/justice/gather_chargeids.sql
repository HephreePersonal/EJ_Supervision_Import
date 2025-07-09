	DROP TABLE IF EXISTS {{DB_NAME}}.DBO.ChargesToConvert
GO
			SELECT DISTINCT		 
				 XCB.ChargeID		 
			INTO {{DB_NAME}}.dbo.ChargesToConvert
			FROM
				Justice.DBO.SUPCASEHDR SCH 
					INNER JOIN Justice.DBO.XCASEBASECHRG XCB ON SCH.CASEID=XCB.CASEID		--Supervision Case ID, match on Case ID
					LEFT  JOIN Justice.DBO.XCASEBASECHRG XCB2 ON XCB.CHARGEID=XCB2.CHARGEID --The related Clerk CaseID is here
GO
		INSERT INTO {{DB_NAME}}.dbo.ChargesToConvert (ChargeID)	
			SELECT DISTINCT		 
				 XCB2.ChargeID		 
			FROM
				Justice.DBO.SUPCASEHDR SCH 
					INNER JOIN Justice.DBO.XCASEBASECHRG XCB ON SCH.CASEID=XCB.CASEID		--Supervision Case ID, match on Case ID
					LEFT  JOIN Justice.DBO.XCASEBASECHRG XCB2 ON XCB.CHARGEID=XCB2.CHARGEID --The related Clerk CaseID is here
			WHERE
				XCB2.ChargeID <> XCB.ChargeID --Only add Clerk Charges that are not already in the table
GO
	ALTER TABLE {{DB_NAME}}.DBO.ChargesToConvert ALTER COLUMN ChargeID INT NOT NULL
GO
	ALTER TABLE {{DB_NAME}}.DBO.ChargesToConvert ADD CONSTRAINT PK_ChargeID PRIMARY KEY (ChargeID)
GO
