	DROP TABLE IF EXISTS {{DB_NAME}}.dbo.DocumentsToConvert
GO
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	INTO {{DB_NAME}}.dbo.DocumentsToConvert
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Bonds that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.Bond C ON A.ParentID=C.BondID 
	WHERE
		B.[Description]='Bonds'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Cases that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.ClkCaseHdr C ON A.ParentID=C.CaseID
	WHERE
		B.[Description]='Case'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupClinicalScreenings that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupClinicalScreening C ON A.ParentID=C.ClinicalScreeningID
	WHERE
		B.[Description]='Clinical Screening'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to CrimDispEvents that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.CrimDispEvent C ON A.ParentID=C.CriminalDispositionEventID
	WHERE
		B.[Description]='Criminal Disposition Event'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Events that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.[Event] C ON A.ParentID=C.EventID
	WHERE
		B.[Description]='Event'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Hearings that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.[HearingEvent] C ON A.ParentID=C.HearingID
	WHERE
		B.[Description]='Hearings'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Jailings that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.Jailing C ON A.ParentID=C.JailingID
	WHERE
		B.[Description]='Jailing'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A
			INNER JOIN Operations.dbo.sParentType B ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupNeedsAssessments that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupNeedsAssessment C ON A.ParentID=C.NeedsAssessmentID
	WHERE
		B.[Description]='Needs Assessment'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Parties that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.Party C WITH (NOLOCK) ON A.ParentID=C.PartyID
	WHERE
		B.[Description]='Party'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Plea Evevnts that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.PleaEvent C WITH (NOLOCK) ON A.ParentID=C.[PleaEventID]
	WHERE
		B.[Description]='Plea Event'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Justice.Pretrial.Interview that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.Pretrial.Interview C WITH (NOLOCK) ON A.ParentID=C.InterviewID
	WHERE
		B.[Description]='Pre-Trial Interview'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupPSI Rows that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupPartyPSI C WITH (NOLOCK) ON A.ParentID=C.PSIID
	WHERE
		B.[Description]='PSI'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupPartyReferral rows that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupPartyReferral C WITH (NOLOCK) ON A.ParentID=C.ReferralPartyID
	WHERE
		B.[Description]='Referral'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupRiskAssessment rows that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupRiskAssessment C WITH (NOLOCK) ON A.ParentID=c.RiskAssessmentID
	WHERE
		B.[Description]='Risk Assessment'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Sentence Evevnts that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SentenceEvent C WITH (NOLOCK) ON A.ParentID=C.SentenceEventID
	WHERE
		B.[Description]='Sentence Event'
UNION
	-- REVIEW REVIEW REVIEDW
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupervisionRec rows that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupervisionRec C WITH (NOLOCK) ON A.ParentID=C.SupervisionRecordID
	WHERE
		B.[Description]='Supervision'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to SupContact records that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.SupContact C WITH (NOLOCK) ON A.ParentID=C.ContactID
	WHERE
		B.[Description]='Supervision Contacts'
UNION
	SELECT
		 A.DocumentID
		,B.ParentTypeID
		,B.[Description] 
	FROM
		Operations.dbo.ParentLink A WITH (NOLOCK)
			INNER JOIN Operations.dbo.sParentType B WITH (NOLOCK) ON A.ParentTypeID=B.ParentTypeID
			-- Only Documents attached to Warrants that are in our scope (note the use of {{DB_NAME}} and not Justice)
			INNER JOIN {{DB_NAME}}.dbo.Wrnt C WITH (NOLOCK) ON A.ParentID=C.WarrantID
	WHERE
		B.[Description]='Warrants'
GO