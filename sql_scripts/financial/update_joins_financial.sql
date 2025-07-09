	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Financial ALTER COLUMN Select_Into TEXT
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Financial ALTER COLUMN Select_Only TEXT
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Financial ALTER COLUMN Joins TEXT
GO
	UPDATE {{DB_NAME}}.dbo.TableUsedSelects_Financial SET Freq=LTRIM(RTRIM(REPLACE(REPLACE(Freq,',',''),'nan',0)))
	UPDATE {{DB_NAME}}.dbo.TableUsedSelects_Financial SET InScopeFreq=LTRIM(RTRIM(REPLACE(REPLACE(InScopeFreq,',',''),'nan',0)))
	UPDATE {{DB_NAME}}.dbo.TableUsedSelects_Financial SET fConvert=LTRIM(RTRIM(REPLACE(REPLACE(fConvert,'.0',''),'nan',0)))
GO
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects_Financial ALTER COLUMN Freq INT NOT NULL
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects_Financial ALTER COLUMN InScopeFreq INT NOT NULL
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects_Financial ALTER COLUMN fConvert BIT NOT NULL
GO
	UPDATE TTC SET
		  Joins			=REPLACE(LTRIM(RTRIM(SUBSTRING(S.SELECT_ONLY,CHARINDEX('A WITH (NOLOCK)',S.SELECT_ONLY)+15,8000))),'() AS YoDate','')
		 ,ScopeRowCount	=S.InScopeFreq
		 ,ScopeComment	=S.Comment
		 ,fConvert		=S.fConvert
	FROM
		{{DB_NAME}}.dbo.TableUsedSelects_Financial S 
			INNER JOIN {{DB_NAME}}.DBO.TablesToConvert_Financial TTC WITH (NOLOCK) ON S.DatabaseName=TTC.DatabaseName AND S.SchemaName=TTC.SchemaName AND S.TableName=TTC.TableName
GO
