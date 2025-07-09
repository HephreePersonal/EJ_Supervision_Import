
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert ALTER COLUMN Select_Into TEXT
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert ALTER COLUMN Select_Only TEXT
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert ALTER COLUMN Joins TEXT
GO
	UPDATE {{DB_NAME}}.dbo.TableUsedSelects SET Freq=LTRIM(RTRIM(REPLACE(REPLACE(Freq,',',''),'nan',0)))
	UPDATE {{DB_NAME}}.dbo.TableUsedSelects SET InScopeFreq=LTRIM(RTRIM(REPLACE(REPLACE(InScopeFreq,',',''),'nan',0)))
	UPDATE {{DB_NAME}}.dbo.TableUsedSelects SET fConvert=LTRIM(RTRIM(REPLACE(REPLACE(fConvert,'.0',''),'nan',0)))
GO
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects ALTER COLUMN Freq INT NOT NULL
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects ALTER COLUMN InScopeFreq INT NOT NULL
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects ALTER COLUMN fConvert BIT NOT NULL
GO
	UPDATE TTC SET
		  Joins			=REPLACE(LTRIM(RTRIM(SUBSTRING(S.SELECT_ONLY,CHARINDEX('A WITH (NOLOCK)',S.SELECT_ONLY)+15,8000))),'() AS YoDate','')
		 ,ScopeRowCount	=S.InScopeFreq
		 ,ScopeComment	=S.Comment
		 ,fConvert		=S.fConvert
	FROM
		{{DB_NAME}}.dbo.TableUsedSelects S 
			INNER JOIN {{DB_NAME}}.DBO.TablesToConvert TTC WITH (NOLOCK) ON S.DatabaseName=TTC.DatabaseName AND S.SchemaName=TTC.SchemaName AND S.TableName=TTC.TableName
GO
	UPDATE {{DB_NAME}}.dbo.TablesToConvert SET Joins='WHERE 1=0' WHERE TableName IN ('AMHearingTypeDefaultHearingNoticeSetup','LawCivilPaperWritCfg')
GO
