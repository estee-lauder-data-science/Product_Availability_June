-- see if any master data tables are stale - RC = 0 is OK
declare @iDays as integer
set @iDays = 30
declare @ct as integer 
set @ct = 0
select @ct = @ct + count(*) from MASTER_DATA.FED_LPR where archLoadDate < getDate() - @iDays
select @ct = @ct + count(*) from MASTER_DATA.LMRPC where archLoadDate < getDate() - @iDays
select @ct = @ct + count(*) from MASTER_DATA.MRP_CONTROLLER where archLoadDate < getDate() - @iDays
select @ct = @ct + count(*) from MASTER_DATA.MVKE_PRAT1 where archLoadDate < getDate() - @iDays
select @ct = @ct + count(*) from MASTER_DATA.SETS where archLoadDate < getDate() - @iDays
select @ct as RC