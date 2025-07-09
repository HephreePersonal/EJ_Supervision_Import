WITH CTE_HEARINGS AS
(
		SELECT
			A.HEARINGID
		FROM
			Justice.dbo.HearingEvent A WITH (NOLOCK)
				INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON A.CaseID=CTC.CaseID
	UNION
		SELECT
			A.HEARINGID
		FROM
			Justice.dbo.HearingEvent A WITH (NOLOCK)
				INNER JOIN {{DB_NAME}}.dbo.ChargesToConvert CTC WITH (NOLOCK) ON A.ChargeID=CTC.ChargeID
	UNION
		SELECT
			A.HEARINGID
		FROM
			Justice.dbo.HearingEvent A WITH (NOLOCK)
				INNER JOIN {{DB_NAME}}.dbo.WarrantsToConvert WTC WITH (NOLOCK) ON A.WarrantID=WTC.WarrantID
)
	SELECT
		A.HearingID
	INTO {{DB_NAME}}.dbo.HearingsToConvert
	FROM
		CTE_HEARINGS A 
	GROUP BY 
		A.HearingID;
GO
	ALTER TABLE {{DB_NAME}}.DBO.HearingsToConvert ADD CONSTRAINT HearingID PRIMARY KEY (HearingID);
GO
