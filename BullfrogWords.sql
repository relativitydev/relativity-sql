--BullfrogWords 
--Author:  Scott R. Ellis  (Scott.ellis@Relativity.com)
--Date Created: 8-13-2012
--Last modified - 12-7-2018
--Description: this is a script that is designed to delimit fields and insert them as single words into a new table.
--This script will create the tables needed.

--NOTE: Before running this script, KNOW YOUR DATA.  If the field you are consuming contains a lot of data, consider adjusting your tempDB log file size.  Ensure that there is at least twice as much space in the TempDB log file as the total size of the consumable.  Ensure that the Tempdbs themselves are at least the size of the comsumbale as well, because a temporary file will be created there that will be a working copy of the releavnt, consumable data.

--NOTES: 
--You can restrict the size of the word being identified.  the upper limit, hard coded, of any word size is 100.  For words that are larger than 100, you may "invert" this script so that it only indexes words that are larger than 100.  Some upper limit should always be set, though.

--Some features are not enabled in this version and exist for future compatability.  

--Previously, this script made use of a now redacted logging procedure.  It has been commented out for purposes of replacing it later using NG logging (slogger and other scope based procedures).  Keep your eyes open for a future release of an SQL Common repo that will host various procedures and functions that scripts like this may leverage - for example, logging and a parallelization maker. 
--This procedure will execute and load the data. 

--EXAMPLE:   
/*  Exec up_chiBullfrogWords --pass in whatever user configurable settings we want, here.  
@fieldConsumable = 'cc'  -- input of the field name on the document table to be worked on. This is the column that will be consumed
,@wordBreak = ';' -- this is the delimiters, which accepts a single delimiter (future version to accept a comma delimited list of delimiters)
,@excludeWords = '' -- Use a pipe delimiter for noise words!
,@maxWordLength = 255 -- -- this is the maximum length of a word, and should be set to the same size as the destination field in your DBMS
	--other stuff
	,@Batchsize = 1000 -- for both carving and for the preload phase, this is the size of the transaction that will be committed.  For the prelaod phase, this is the size of the actual number of rows that will be inserted into the temp table to be worked on.  
	,@withResume = 0 --Resume the operation. OK, so, if for some reason your attempt fails and you want it to pickup where you left off, set this to '1'.  Otherwise, the temp table you just spent three hours building will be lost and it will do it again.
	NOTE- this script is designed to work with Relativity's document table. Manually alter the procedure to use a different table if desired.
	
	EXAMPLE (This is a sample of useful queries when run in standalone mode)
	-------------------------------------------------------
	
	--The number of distinct email addresses in the set
	SELECT  count (distinct word) from [ReadyTest].[BullfrogIDCWords]
	
	  --Total rows in starting table: 22,980,761
	--Total rows in starting table that are null : 21,077,446
	--Total Rows examined: 1,903,315
	--Total email addresses : 17,423,624
	--Total distinct email addresse: 268,934
	
	SELECT COUNT (artifactID) from doclittle where cc is NULL

--This will pull back the number of distinct pairs
SELECT count(Distinct checksum(word, artifactID)) from [BullfrogIDCWords]

--this brings back the number of emails per artifactID. 
SELECT COUNT (ArtifactID) as countz, artifactID from [BullfrogIDCWords] group by artifactID order by countz DESC
------------------------------
 */

 --Choose a database where you want this procedure to reside
USE msdb
IF EXISTS (select * from dbo.sysobjects where id = object_id(N'[sp_Bullfrog]') and OBJECTPROPERTY(id, N'IsProcedure') = 1)  
DROP PROCEDURE sp_Bullfrog

GO
CREATE PROCEDURE sp_Bullfrog --pass in whatever user configurable settings we want, here. 
	@tableConsumable nvarchar (100), --the name of the table upon which the column to be consumed resides.
	@IDConsumable nvarchar (100), --this is the ID in the table that can be used to look up the row to consume
	@fieldConsumable nvarchar (100) -- input of the column name on the document table to be worked on
	,@wordBreak nvarchar (20) -- this is the delimiters, which accepts a comma delimited list of delimiters (not completed for this first version)
	,@excludeWords nvarchar (1000) -- this is a comma delimited list of words you want to exclude. 
	--other stuff
	,@EDDSPerformanceServer nvarchar (150)
	,@VSRunID int = 0 
	,@AgentID int = -1
	,@Batchsize int = 1000-- this is the size of the transactions that will accumulate before being committed, and the size of the set operation when moving records to tempDB to work on them  
	,@withResume bit = 0 --Resume the operation. If for some reason your attempt fails and you want it to pickup where you left off, set this to '1'.  Otherwise, for large jobs, the temp table you just spent three hours building will be lost and it will do it again. This is of course for LARGE operations, not the piddly little business of counting conditions on a search. 
	,@logging bit = 0 
AS
DECLARE
	@runStart datetime = getUTCDate(),
	@entryStartUTC datetime = getUTCdate(),
	@o INT, --outer loop to control how many batches.   
	@i INT = 1,--Inner loop to control the start and stop of a single batch counter.
	@ErrorMessage varchar(max),
	@FAILED BIT = 0,
	@CarvedText nvarchar (100), --the longest word is restricted here to 100 characters
	@iMAX INT, --MAX VALUE
	@SQL nVARCHAR(MAX),
	@quit INT,
	@ExText nvarchar(max),
--This is the text to examine. This engine can handle up to 2GB of text per bite, where each word is a string of letters and characters.
	@SQLText nvarchar (max),
--Parsing Variables
	@tableConsumableID int,
	@Pos_exText int, --this is the position in the @exText variable being examined.
	@WordCount int,
	@Position INT,
	@LoggingVars nvarchar(max)

--IF @logging = 1 
--	BEGIN
--		SET @loggingVars = '-'
--		EXEC EDDSQoS.QoS_LogAppend
--				@EDDSPerformanceServer = @EDDSPerformanceServer,
--				@runStartUTC= @runStart,
--				@AgentID = @AgentID,
--				@EntryStartUTC = @entryStartUTC,
--				@module = 'QoS_Bullfrog',
--				@taskCompleted = 'Bullfrog successfully called, declarations completed, logging errors only',
--				@otherVars = @loggingVars,
--				@nextTask =  ''
--		SET @entryStartUTC = getUTCdate()
--	END

--avoid clustered index scans.  We never expect more than one VRHID to be here, but it could be more:

--Gets the total number of items that will be worked on.  
--The larger the consumable, and the smaller the (relative) amount of RAM, the slower this will go.  At some point, it may make sense to batch through even this query.  Why can't we get an index on the NULL bitmap?  Explore moving the field to a fixed length field that can be indexed. For years, Relativity users have been putting these fields into long text fields, needed or not needed, and they don't perform as well as fixed length text fields, and they can't be indexed. 

--PARSING TABLES
--CREATE the Bullfrog Words Framework
IF NOT EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'BullfrogWork')
BEGIN
	CREATE TABLE BullfrogWork (
		BFWID INT IDENTITY(1,1) PRIMARY KEY
		,IDConsumable INT
		,VRHID INT
	)
	CREATE NONCLUSTERED INDEX [IDConsumable] ON BullfrogWork
	(
	VRHID ASC,
	IDConsumable ASC
	)
END
IF NOT EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'BullfrogIDCWords')
BEGIN
	CREATE TABLE BullfrogIDCWords
	(
	WordID INT IDENTITY (1,1),
	BFWID INT, --bullfrogworkID
	IDConsumable INT, 
	Word nvarchar(100), -- this is here only as a reference - it should never be used as it will not be nearly so fast to search this as to search the word table, which is the normal form of this column.
	Position INT
	)
	CREATE CLUSTERED INDEX [CI_IDC_Word] ON BullfrogIDCWords
	(
		IDConsumable ASC,
		[WordID] ASC
	)
	CREATE NONCLUSTERED INDEX [IX_Word] ON BullfrogIDCWords
	(
		[Word] ASC
	)
END
--this table is a lightweight, lookup table of the artifactIDs.  It is used to count the number of records to be reviewed, and is used to allow a lookup of them by artifactID.
--This table will be loaded with the items to work on. We are assuming that the tempDBs have faster disk speed, and this is a laborious operation so we want to have fast access to this table, especially.
IF object_ID('#BullfrogPreload') Is NULL
	CREATE TABLE #BullfrogPreload (
		IDConsumable int NOT NULL PRIMARY KEY  --this will receive the ID of any row that is intended to be parsed from the source table.
		,FieldConsumable nvarchar(max) --this is the delimited list of items to be parsed into a separate table.
		)
	--this table will store a unique list of words, this is your dictionary.
IF object_ID('#BullfrogWords') Is NULL
BEGIN
	CREATE TABLE #BullfrogWords  (
		WordID INT IDENTITY(1,1),
		Word nvarchar(100),
		BackWord nvarchar(100) -- not used yet --intent was to reverse the order of the word here for some fancy searching, or even to help parallelize operations by working at the column from both sides. 
		)
	CREATE CLUSTERED INDEX [CI_WordID] ON #BullfrogWords(
		[WordID] ASC
		)
	CREATE NONCLUSTERED INDEX [NCI_word] ON #BullfrogWords(
		[Word] ASC
		)
--this was placed here to prevent lockescalation on the source table. 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
END
SET @FAILED = 0
--CREATE a temp table for  foundation
--WARNING - It is important that you be the only one running this script in a workspace.  If anyoen else is running this scrpt when you arerunning it, their work will be destroyed.  Previous runs will also be destroyed.  You must rename any permanent tables that you want to keep. 

--IF @logging = 1 
--BEGIN
--	EXEC EDDSQoS.QoS_LogAppend
--	@EDDSPerformanceServer = @EDDSPerformanceServer,
--	@runStartUTC = @runStart,  -- this is used to calculate the interval length, in milliseconds
--	@AgentID = @AgentID,
--	@entryStartUTC = @entryStartUTC,
--	@module = 'QoS_Bullfrog',
--	@taskCompleted = 'Created tables and indexes',
--	@otherVars = '',
--	@nextTask = 'Loading data to BullfrogWork table'
--	SET @entryStartUTC = getUTCdate()
--END		
--To Modify Input data parameters: if you wanted to make a more specific query to pull data back to be reviewed, you owuld modify this query here. Run the saved search, then capture the saved search query from the History tab, and insert it below the after the INSERT INTO statement.  Be sure not to lose the @fieldConsumable - you will still need that to run this.
SELECT @SQLText = N'INSERT INTO BullfrogWork SELECT [' + @IDConsumable + '], ' + CAST(@VSRunID as nvarchar(10)) + ' FROM ' + @tableConsumable + ' WHERE [' + @fieldConsumable + '] IS NOT NULL order by [' + @IDConsumable + ']'
--PRINT @SQLText
EXEC sp_executesql @SQLText 
--SELECT COUNT (*) FROM BullfrogWork	

--here is logic to get this to start where it left off.  @i is be set to the Artifact ID in the BullfrogWork Table that is the next one after the highest one in the ## temp table. 
IF @withResume = 0
	SET @i = 1
ELSE
	SELECT @i = BFWID + 1 FROM BullfrogWork WHERE IDConsumable = (SELECT TOP 1 IDConsumable FROM #BullfrogPreload order by IDConsumable DESC)
IF @i = NULL
	SET @i = 1
SET @quit = 0
--PULL the max ID from the temp table here.
SET @iMAX = (SELECT TOP 1 BFWID FROM BullfrogWork ORDER by BFWID DESC)--the highest ID numbered record you want to move. --this is the max @IDconsumable record from @Tableconsumable
IF @iMAX < @BatchSize 
	SET @o = @iMAX
ELSE
	SET @o = @BatchSize

WHILE @o <= @iMAX and @quit <> 1 AND @FAILED = 0
BEGIN
	
	SET NOCOUNT ON

	IF @quit <> 1
	BEGIN 
		IF @iMAX = @o
		SET @quit = 1
	
		BEGIN TRAN  --if any part of this fails, the whole thing rolls back
		BEGIN TRY  
			--INSERT Processing SQL HERE
			SET @SQLText = 'INSERT INTO #BullfrogPreload (IDConsumable, FieldConsumable) SELECT [' + @IDConsumable + '], REPLACE([' + @FieldConsumable + '], '''''''', '''') FROM ' + @tableConsumable + ' WHERE ' + @IDConsumable + ' IN (SELECT IDConsumable FROM BullfrogWork WHERE BFWID BETWEEN ' + cast(@i as nvarchar(10)) + ' AND ' + CAST(@o as nvarchar(10)) + 'and VRHID = ' + convert(nvarchar(10),@VSRunID) + 	 ')
			OPTION (maxdop 2)'
		
			EXEC sp_executesql @SQLText  
		END TRY
		--Begin Error handling
		BEGIN CATCH
			IF @@TranCount > 0
			ROLLBACK TRAN
			SET @quit = 1 
			SET @ErrorMessage = 'Message' + CONVERT(varchar(250), ERROR_MESSAGE()) + 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
				', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) + 
			', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
			', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
			', Line ' + CONVERT(varchar(5), ERROR_LINE());
	--	IF @logging = 1 
	--	BEGIN
	--		SET @loggingVars = 'FAILED SQL: ' + @SQL + '; --ErrorMessage = ' + @ErrorMessage
	--		EXEC EDDSQoS.QoS_LogAppend
		--		@EDDSPerformanceServer = @EDDSPerformanceServer,
		--		@runStartUTC= @runStart,
		--		@AgentID = @AgentID,
		--		@EntryStartUTC = @entryStartUTC,
		--		@module = 'QoS_Bullfrog',
		--		@taskCompleted = 'Completed round 1 sub-search analysis',
		--		@otherVars = @loggingVars,
		--		@nextTask =  'Begin round 2 sub-search analysis'
	--		SET @entryStartUTC = getUTCdate()
	--	END	
			SET @FAILED = 1
		END CATCH
		IF @@TRANCOUNT > 0
		COMMIT TRANSACTION
		IF @quit <> 1
		BEGIN
			SET @i = @i + @BatchSize --increment the next begin coutner
			SET @o = @o + @BatchSize  --increment the next end counter
			IF @o > @iMax --if @o is too big, this means that we are at the tail of the job
			SET @o = @iMAX --@i + the batchsize would push us over the boundary, so set the upper limit of this pass to be the same as the max value to be worked on.
		END	

	END
END

--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
--                                     PARSE											  --
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------


SET @quit = 0
SET @i = 1  
--PULL the max ID from the temp table here.
SET @iMAX = (SELECT TOP 1 BFWID FROM BullfrogWork ORDER by BFWID DESC)--the highest ID numbered record you want to move. --this is the max ID record from AuditRecord 
IF @iMAX < @BatchSize 
	SET @o = @iMAX
ELSE
	SET @o = @BatchSize
--batching the carve phase into transactions for rollback ability.
--First, get the exceptions into a temp table (if there are any)
DECLARE @noise table (noiseword nvarchar (100) NOT NULL) 
IF @excludeWords <> ''
BEGIN
	SELECT @ExText = LTRIM(RTRIM(@excludewords)) 
	SET @exText = LTRIM(RTRIM(@ExText)) + '|'
	SET @Pos_exText = CHARINDEX(@wordBreak, @exText, 1)
	SET @Position = 0
	WHILE @pos_Extext > 0
	BEGIN
		SET @Position = @Position + 1 
		SET @carvedText = LTRIM(RTRIM(LEFT(@ExText, @Pos_exText - 1)))
		IF @carvedText <> ''
		BEGIN 
			INSERT INTO @noise VALUES (@CarvedText) 
		END
		SET @ExText = SUBSTRING(@ExText, @Pos_exText + 1, LEN(@ExText))
		SET @pos_ExText = CHARINDEX(@wordBreak, @ExText, 1)
	END
END	
	
--Now, process this batch!

WHILE @o <= @iMAX and @quit <> 1 AND @FAILED = 0
BEGIN		
	SET NOCOUNT ON

	IF @quit <> 1
	BEGIN 
		IF @iMAX = @o
		SET @quit = 1
	
		BEGIN TRAN  --if any part of this fails, the whole thing rolls back
		BEGIN TRY  
		WHILE @i <= @o
		BEGIN
		--INSERT Processing SQL HERE
		--Carve each field into the BullfrogIDCWords table
		--This should always be run prior to a new run, so that the procedure won't get confused. This will destroy any existing tables, so rename things you want to keep!!!
--WHERE BFWID BETWEEN ' + CONVERT(nvarchar(30),@i) + ' AND ' + convert(nvarchar(30),@o) + ')	OPTION (maxdop 4)

			SELECT @ExText = LTRIM(RTRIM(FieldConsumable)) FROM #BullfrogPreload WHERE IDConsumable = (SELECT IDConsumable FROM BullfrogWork WHERE BFWID = @i)
			SELECT @tableConsumableID = IDConsumable FROM BullfrogWork WHERE BFWID = @i
			SET @exText = LTRIM(RTRIM(@ExText)) + @wordBreak
			--later on, this can be a while loop that goes through each of the delimeters called in the procedure call
			SET @exText=REPLACE(@exText,',',' ')
			SET @exText=REPLACE(@exText,CHAR(13),' ')
			SET @exText=REPLACE(@exText,CHAR(9),' ')
			SET @exText=REPLACE(@exText,CHAR(10),' ')
			SET @exText=REPLACE(@exText,CHAR(9),' ')
			SET @Pos_exText = CHARINDEX(@wordBreak, @exText, 1)
			SET @Position = 0
			WHILE @pos_Extext > 0
			BEGIN
				SET @Position = @Position + 1 
				SET @carvedText = LTRIM(RTRIM(LEFT(@ExText, @Pos_exText - 1)))
				SELECT @carvedText = '' FROM @noise WHERE @carvedText IN (SELECT nw.noiseWord FROM @noise nw)
				IF @carvedText <> ''
				BEGIN
				INSERT INTO [BullfrogIDCWords] (IDConsumable, BFWID, Word, Position) VALUES (@tableConsumableID,@i,@CarvedText,@Position) 
				END
				SET @ExText = SUBSTRING(@ExText, @Pos_exText + 1, LEN(@ExText))
				SET @pos_ExText = CHARINDEX(@wordBreak, @ExText, 1)
			END
			SET @WordCount = @WordCount + @Position
			SET @i = @i + 1
		END 

		END TRY
		--Begin Error handling
		BEGIN CATCH
			IF @@TranCount > 0
			ROLLBACK TRAN
			SET @quit = 1 
			SET @ErrorMessage = 'Message' + CONVERT(varchar(250), ERROR_MESSAGE()) + 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
				', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
			', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
			', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
			', Line ' + CONVERT(varchar(5), ERROR_LINE());
	--IF @logging = 1 
	--BEGIN
	--	SET @loggingVars = 'Probable failed text: ' + @extext + '; --ErrorMessage = ' + @ErrorMessage
	--	EXEC EDDSQoS.QoS_LogAppend
	--			@EDDSPerformanceServer = @EDDSPerformanceServer,
	--			@runStartUTC= @runStart,
	--			@AgentID = @AgentID,
	--			@EntryStartUTC = @entryStartUTC,
	--			@module = 'QoS_Bullfrog',
	--			@taskCompleted = 'Completed round 1 sub-search analysis',
	--			@otherVars = @loggingVars,
	--			@nextTask =  'Begin round 2 sub-search analysis'
	--	SET @entryStartUTC = getUTCdate()
	--END	
			SET @FAILED = 1
		END CATCH
		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION
		IF @quit <> 1
		BEGIN
				--No need to increment the next counter because it's already incremented.
			SET @o = @o + @BatchSize  --increment the next end counter
			IF @o > @iMax --if @o is too big, this means that we are at the tail of the job
			SET @o = @iMAX --@i + the batchsize would push us over the boundary, so set the upper limit of this pass to be the same as the max value to be worked on.
		END	
	END
END

--carve out the items here, creating a single list of all items  [BullfrogIDCWord]
--dedupe and create the dictionary.  #BullfrogWords
--assign the items in the BullfrogIDCWord table a wordID  -- to prevent boundary conditions, this batch will have a different numbering method, and a delete statement.  for example, 
/*it will dedupe 1-100, then delete item 100 from BullfrogWords.  
Then it will dedupe 100 - 200, 
then delete items 100.  
*/