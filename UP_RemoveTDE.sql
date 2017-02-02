

USE [EDDSMaintenance]

IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'RemoveTDE')
DROP PROCEDURE [dbo].RemoveTDE
go
CREATE PROCEDURE [dbo].RemoveTDE
    @dbname varchar (100) = NULL,
	@Execute bit = 0,
	@doWithDRBackup bit = 1 --Takes a full copy Only backup before decrypting in the event something goes wrong
AS    
DECLARE @SQL nvarchar(max)
DECLARE @aoagname nvarchar(30)
if @dbname is NULL
PRINT '--sample usage: 
EXEC [dbo].RemoveTDE  @dbname =EDDS1024176,
	@Execute = 1, -- when ''1'', this will actually execute the process.  Otherwise, it only prints the commands to be run
	@doWithDRBackup = 1 --when set to ''1'', this takes a copy only backup of the database.'
--Remove if the database is part of an AG
IF sys.fn_hadr_backup_is_preferred_replica(@dbname) = 1
Begin
	SELECT @aoagName = AG.name
		FROM master.sys.availability_groups AS AG
		INNER JOIN master.sys.availability_replicas AS AR
					ON AG.group_id = AR.group_id
		INNER JOIN master.sys.dm_hadr_availability_replica_states AS arstates
					ON AR.replica_id = arstates.replica_id AND arstates.is_local = 1
		INNER JOIN master.sys.dm_hadr_database_replica_cluster_states AS dbcs
					ON arstates.replica_id = dbcs.replica_id
		WHERE dbcs.database_name = @dbname
		--kill all existing connections and remove from AOAG
	SET @SQL = '
					 ALTER AVAILABILITY GROUP ' + quotename(@aoagName) + '
                     REMOVE DATABASE ' + QUOTENAME(@dbname) + ';
					 ALTER DATABASE ' + QUOTENAME(@dbname) + ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
					  ALTER DATABASE ' + QUOTENAME(@dbname) + ' SET MULTI_USER WITH ROLLBACK IMMEDIATE;'
	IF @Execute = 1
		EXEC sp_executeSQL @SQL
	ELSE
		PRINT @SQL                 

END
-- Take a full backup to protect you in case something goes horribly wrong with the key drop.  This will also get rid of any encrypted logs.
SET @sql = 'BACKUP DATABASE ' + @dbname + ' TO  DISK = N''M:\Backups\' + @dbname + 'B.bak'' WITH NOFORMAT, NOINIT,  NAME = N''' + @dbname + '_' + REPLACe((REPLACe(convert(varchar(500),GetDate(),120),' ', '_')),':','.') + '-FullDatabaseBackupCopyOnly'', SKIP, NOREWIND, NOUNLOAD, COPY_ONLY, STATS = 10'
If @execute = 1 and @doWIthDRBackup = 1 
EXEC sp_executeSQL @SQL
PRINT @SQL
--take it out of the AG
--connect to the replica
--turn off encryption
--drop this database from the replica
SET @SQL = '
USE ' + @dbname + ';
ALTER DATABASE ' + @dbname + ' SET ENCRYPTION OFF;

WHILE EXISTS (
SELECT 1  FROM master.sys.dm_database_encryption_keys WHERE encryption_state = 5 and db_name(database_ID) = ''' + @dbname + ''')
BEGIN
SELECT Encryption_state  FROM master.sys.dm_database_encryption_keys WHERE db_name(database_ID) = ''' + @dbname + '''
WAITFOR DELAY ''00:00:05''
RAISERROR(''decrypting still'', 10, 1) WITH NOWAIT
END

DROP DATABASE ENCRYPTION KEY'

If @execute = 1 
EXEC sp_executeSQL @SQL
ELSE
PRINT @SQL

