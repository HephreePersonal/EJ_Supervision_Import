	SELECT A.EventID 
		INTO {{DB_NAME}}.dbo.EventsToConvert FROM Justice.dbo.CaseEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID UNION 
	SELECT A.CriminalDispositionEventID FROM Justice.dbo.CrimDispEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID UNION 
	SELECT A.HearingID FROM Justice.dbo.HearingEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID UNION
	SELECT A.JudgmentEventID FROM Justice.dbo.JudgmentEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID UNION
	SELECT A.EventID FROM Justice.dbo.OffHist A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.ChargesToConvert CTC WITH (NOLOCK) ON A.ChargeID=CTC.ChargeID WHERE A.EventID IS NOT NULL UNION
	SELECT A.PleaEventID FROM Justice.dbo.PleaEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID UNION
	SELECT A.SentenceEventID FROM Justice.dbo.SentenceEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID UNION
	SELECT A.ServiceEventID FROM Justice.dbo.SrvcEvent A WITH (NOLOCK) INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID
GO
	ALTER TABLE {{DB_NAME}}.DBO.EventsToConvert ALTER COLUMN EventID INT NOT NULL
GO
	ALTER TABLE {{DB_NAME}}.DBO.EventsToConvert ADD CONSTRAINT PK_EventID PRIMARY KEY (EventID)
GO
