DROP TABLE IF EXISTS {{DB_NAME}}.dbo.TablesToConvert_Operations;

SELECT 
    'Operations' AS [DatabaseName],
    s.[NAME] AS [SchemaName],
    t.[NAME] AS [TableName],
    p.[rows] AS [RowCount],
	CAST(0 AS BIGINT) AS [ScopeRowCount],
    CAST('' AS VARCHAR(8000)) AS [ScopeComment],
    CAST(1 AS BIT) AS [fConvert],
    'SELECT DISTINCT ' + 
    STUFF((
        SELECT ', ' + 
            CASE 
                WHEN ty.[NAME] IN ('Comment','Cmmnt','text', 'ntext','xml','image') 
                THEN 'CAST(A.' + QUOTENAME(c.[NAME]) + ' AS NVARCHAR(MAX)) AS ' + QUOTENAME(c.[NAME])
                ELSE 'A.' + QUOTENAME(c.[NAME])
            END
        FROM 
			Operations.sys.columns c
				INNER JOIN Operations.sys.types ty ON c.user_type_id=ty.user_type_id
        WHERE c.object_id=t.object_id
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + 
    ' INTO {{DB_NAME}}.dbo.Operations_' + T.[NAME] + ' FROM Operations.' + QUOTENAME(s.[NAME]) + '.' + QUOTENAME(t.[NAME]) +
    ' A WITH (NOLOCK) ' AS [Select_Into],
	'DROP TABLE IF EXISTS {{DB_NAME}}.dbo.Operations_' + T.[NAME]  AS Drop_IfExists,
	'SELECT DISTINCT A' + 
    STUFF((
        SELECT ',' + 
            CASE 
                WHEN ty.[NAME] IN ('Comment','Cmmnt','text', 'ntext','xml','image') 
                THEN 'CAST(A.' + QUOTENAME(c.[NAME]) + ' AS NVARCHAR(MAX)) AS ' + QUOTENAME(c.[NAME])
                ELSE 'A.' + QUOTENAME(c.[NAME])
            END
        FROM 
			Operations.sys.columns c
				INNER JOIN Operations.sys.types ty ON c.user_type_id=ty.user_type_id
        WHERE c.object_id=t.object_id
        ORDER BY c.column_id
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + 
      + ' FROM Operations.' + QUOTENAME(s.[NAME]) + '.' + QUOTENAME(t.[NAME]) +
    ' A WITH (NOLOCK) WHERE 1=0' AS Select_Only,
	CAST('' AS VARCHAR(8000)) AS Joins
INTO {{DB_NAME}}.dbo.TablesToConvert_Operations
FROM 
    Operations.sys.tables t
		INNER JOIN Operations.sys.schemas s ON t.schema_id=s.schema_id
		INNER JOIN Operations.sys.indexes i ON t.object_id=i.object_id AND i.index_id <= 1
		INNER JOIN Operations.sys.partitions p ON i.object_id=p.object_id AND i.index_id=p.index_id
WHERE 
    t.is_ms_shipped=0 -- Exclude system tables
    AND t.[type]='U'  -- User tables only
ORDER BY 
    s.[NAME], t.[NAME];

ALTER TABLE {{DB_NAME}}.dbo.TablesToConvert_Operations ADD RowID INT IDENTITY(1,1);
