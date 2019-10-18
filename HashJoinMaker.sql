/* Background: Prior to Relativity version ~8.0, Relativity had a system level configuration in the EDDS database that allowed 
    administrators to set Hashjoin to true or false, globally. 
    It was removed due to the fact that while the hash join hint would make some workspaces relational queries faster, and some slower.
    This trigger allows you to "turn on" hash joins within single workspaces, where you have been advised or have figured out on your own
    that it is needed. 
   
*/

--IMPORTANT : You must read all of the comments below and fully understand, and execute, any pre-requisite action items.  
    
USE [EDDS######] --target the workspace that needs the hash flipper here
IF object_id('HashJoinMaker') is not null
  DROP TRIGGER HashJoinMaker
GO
CREATE TRIGGER [EDDSDBO].HashJoinMaker ON [EDDSDBO].[View]
    AFTER INSERT, UPDATE
AS
  --NOTE: This trigger uses a case sensitive version of the query hint 'Hashjoin:TrUe' in order to differentiate its hints from those entered by others.
        --If the inserted value of [ViewbyFamily] > 1, set the hashjoin:true hint, unless the QueryHint already contains a string, in which case the user trumps. 
/*
What does this script do? If the:
	1.	User creates a search with family – Hash join hint added automatically.
	2.	User adds family to an existing search – hash join is added automatically.
	3.	User removes family from an existing search – hash join is removed automatically.
	4.	User removes the hash join hint – hash join hint stays removed.
	5.	User inputs a different hint – different hint does not change.
	6.	Someone manually updates a group of rows – all rows are affected in a set operation and the trigger should only fire once
	7.  If Family is removed, and the queryHint field still has the Hashjoin:true hint, the hint will be removed IF the hint is already in the database.  If the hint is NOT already in the database, it will be added -- If they still want the hint, they will have to come back in and re-add it. 
	8.  If a user has input a query hint that is NOT EXACTLY MATCHING (case sensitive) 'Hashjoin:TrUe', it will not be changed.  Ever.   (Notice, if you will, the capital 'U' - it is not a typo)
	9.	If you have used other hints, and Family is added, you will lose your other hints as they may not be compatible with the hashmatch query. 

Written by Scott R. Ellis to solve a very unique problem with a particular, misbehaving workspace. 

ACTION ITEM:  IF you are implementing this trigger after ALREADY using the hashjoin:true hint, you MUST run this UPDATE:
	UPDATE EDDSDBO.[View] SET queryhint = 'Hashjoin:TrUe' WHERE queryhint = 'hashjoin:true'
*/
    IF UPDATE(ViewByFamily)
        BEGIN
            IF 			
			--If the query hint is not set and Family is being set for the first time and this is a fresh insert.  (sastisifies condition 1)
				--the inserted value for ViewByFamily is being set.
				( SELECT [ViewByFamily] FROM   Inserted ) <> 0 
                AND 
				--the inserted value for the QueryHint field is not set
				DATALENGTH((SELECT QueryHint FROM INSERTED )) = 0
				AND 
				--the existing ViewbyFamily value for this search is NOT set
				NOT EXISTS(SELECT [ViewByFamily] FROM DELETED WHERE ArtifactID in (SELECT artifactID FROM Inserted)) 
				--Set the Hashjoin:True value
			Begin
                UPDATE  [EDDSDBO].[View]
                SET     QueryHint = 'Hashjoin:TrUe'
                WHERE   ArtifactID IN ( SELECT  ArtifactID FROM INSERTED)
			END
            IF 
			
			--If the query hint is not set and Family is being set for the first time.  (sastisifies condition 1)
				--the inserted value for ViewByFamily is being set.
				( SELECT [ViewByFamily] FROM   Inserted ) <> 0 
                AND 
				--the inserted value for the QueryHint field is not set
				DATALENGTH((SELECT QueryHint FROM INSERTED )) = 0
				AND 
				--the existing ViewbyFamily value for this search is NOT set
				(SELECT [ViewByFamily] FROM DELETED WHERE ArtifactID in (SELECT artifactID FROM Inserted)) = 0
				--Set the Hashjoin:True value
			Begin
                UPDATE  [EDDSDBO].[View]
                SET     QueryHint = 'Hashjoin:TrUe'
                WHERE   ArtifactID IN ( SELECT  ArtifactID
                                        FROM    INSERTED)
			END
			ELSE
			--if the inserted value of [ViewByFamily] = 0 and the hashjoin hint is Hashjoin:TrUe AND the existing value in the View table is Hashjoin:TrUe, remove the hashjoin:TrUe hint
			IF 
			--the inserted value for ViewByFamily is being set.
				( SELECT [ViewByFamily] FROM   Inserted ) = 0 
                AND 
				--the inserted value for the QueryHint field is not set
				( SELECT cast(QueryHint as varbinary(50)) FROM INSERTED ) = CAST('Hashjoin:TrUe' as varbinary(50))
				AND 
				--the existing ViewbyFamily value for this search is NOT 'Hashjoin:TrUe' 
				(SELECT CAST(QueryHint as varbinary(50)) FROM DELETED WHERE ArtifactID in (SELECT artifactID FROM Inserted)) = CAST('Hashjoin:TrUe' as varbinary(50))
				--Set the Hashjoin:True value
                UPDATE  [EDDSDBO].[View]
                SET     QueryHint = ''
                WHERE   ArtifactID IN ( SELECT  ArtifactID
                                        FROM    INSERTED);
			ELSE
			--if the user has removed the hashjoin, set it to whatever they have set it, regardless of any other values
			IF 
			--the inserted value for ViewByFamily is alread set AND is not being removed
				(( SELECT [ViewByFamily] FROM DELETED WHERE ArtifactID in (SELECT artifactID FROM Inserted) ) <> 0 AND ( SELECT [ViewByFamily] FROM   Inserted ) <> 0)
                AND 
				--the inserted value for the QueryHint field is not the trigger generated hint.
				( SELECT cast(QueryHint as varbinary(50)) FROM INSERTED ) <> CAST('Hashjoin:TrUe' as varbinary(50) )
                WAITFOR DELAY '0:0' --do nothing, this is a sinkhole. 
        END;  
