
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Operations ALTER COLUMN Select_Into TEXT;
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Operations ALTER COLUMN Select_Only TEXT;
	ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Operations ALTER COLUMN Joins TEXT;

	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects_Operations ALTER COLUMN Freq INT NOT NULL;
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects_Operations ALTER COLUMN InScopeFreq INT NOT NULL;
	ALTER TABLE {{DB_NAME}}.dbo.TableUsedSelects_Operations ALTER COLUMN fConvert BIT NOT NULL;

	UPDATE TTC SET
		  Joins			=REPLACE(LTRIM(RTRIM(SUBSTRING(S.SELECT_ONLY,CHARINDEX('A WITH (NOLOCK)',S.SELECT_ONLY)+15,8000))),'() AS YoDate','')
		 ,ScopeRowCount	=S.InScopeFreq
		 ,ScopeComment	=S.Comment
		 ,fConvert		=S.fConvert
	FROM
		{{DB_NAME}}.dbo.TableUsedSelects_Operations S 
			INNER JOIN {{DB_NAME}}.DBO.TablesToConvert_Operations TTC WITH (NOLOCK) ON S.DatabaseName=TTC.DatabaseName AND S.SchemaName=TTC.SchemaName AND S.TableName=TTC.TableName;
