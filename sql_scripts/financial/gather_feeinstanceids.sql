	DROP TABLE IF EXISTS {{DB_NAME}}.dbo.FeeInstancesToConvert
GO
	SELECT DISTINCT
		 FI.FeeInstanceID
	INTO {{DB_NAME}}.dbo.FeeInstancesToConvert
	FROM
		Financial.dbo.FeeInst FI WITH (NOLOCK)
				INNER JOIN Financial.dbo.xChrgFeeInst XCFI WITH (NOLOCK) ON FI.FeeInstanceID=XCFI.FeeInstanceID
				INNER JOIN {{DB_NAME}}.dbo.ChargesToConvert CTC WITH (NOLOCK) ON XCFI.ChargeID=CTC.ChargeID
				INNER JOIN {{DB_NAME}}.dbo.xCaseBaseChrg XCBC WITH (NOLOCK) ON CTC.ChargeID=XCBC.ChargeID
				INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC2 WITH (NOLOCK) ON XCBC.CaseID=CTC2.CaseID AND CTC2.TYPEOFCASE='SUPCASEHDR'
UNION
	SELECT DISTINCT
		 FI.FeeInstanceID
	FROM
		Financial.dbo.FeeInst FI WITH (NOLOCK)
			INNER JOIN Financial.dbo.xFincChrgFeeInst XFCFI WITH (NOLOCK) ON FI.FeeInstanceID=XFCFI.FeeInstanceID
			INNER JOIN Financial.dbo.FincChrg FC WITH (NOLOCK) ON XFCFI.ChargeID=FC.ChargeID
			INNER JOIN Financial.dbo.xCaseFincChrg XCFC WITH (NOLOCK) ON FC.ChargeID=XCFC.ChargeID
			INNER JOIN {{DB_NAME}}.dbo.CasesToConvert CTC WITH (NOLOCK) ON XCFC.CaseID=CTC.CaseID AND CTC.TYPEOFCASE='SUPCASEHDR'
GO