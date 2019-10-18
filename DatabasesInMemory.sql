USE [master]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_DatabasesInMemory]
AS

-- Note: querying sys.dm_os_buffer_descriptors
-- requires the VIEW_SERVER_STATE permission.
--create table 
IF NOT EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DatabasesInMemory') 
BEGIN
		CREATE TABLE [dbo].[ObjectsInMemory](
		[ID] INT Identity(1,1) PRIMARY KEY,
		[SampleDate] [datetime] NOT NULL,
		[DbName] [nvarchar](128) NULL,
		[DbBufferPages] [bigint] NULL,
		[DbBufferMB] [bigint] NULL,
		[DbBufferPercent] [decimal](6, 3) NULL
	) ON [PRIMARY]
END
DECLARE @totalBuffer INT;
SELECT @totalBuffer = cntr_value
FROM sys.dm_os_performance_counters
WHERE RTRIM([object_name]) LIKE '%Buffer Manager'
	AND counter_name = 'Database Pages';

WITH src
	AS (
	SELECT database_id,
			db_buffer_pages = COUNT_BIG(*)
	FROM sys.dm_os_buffer_descriptors
	GROUP BY database_id)
	INSERT INTO dbo.DatabasesInMemory
			SELECT GETUTCDATE() AS 'SampleDate',
				[DbName] = CASE [database_id] WHEN 32767 THEN 'Resource DB' ELSE DB_NAME([database_id]) END,
				db_buffer_pages,
				db_buffer_MB = db_buffer_pages / 128,
				db_buffer_percent = CONVERT(DECIMAL(6, 3), db_buffer_pages * 100.0 / @totalBuffer)
			FROM src;

DELETE dbo.DatabasesInMemory WHERE SampleDate < DATEADD(DD, -32, GETUTCDATE());
GO
