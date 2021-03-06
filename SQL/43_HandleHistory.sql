-- deletes any existing cycle recs from HISTORY (in case of a rerun in the same week)
-- adds this weeks cycle recs to HISTORY
--
-- remove anything over  8 weeks from MAIN
-- remove anything over 52 weeks HISTORY

-- Determine Cycle and Run_Type
Declare @dToday as Date
DECLARE @dCycle AS DATE;
DECLARE @dCycleMinus8 AS DATE;
DECLARE @dCycleMinus52 AS DATE;
Declare @iDoW Integer


Select @dToday = GetDate()
Select @iDoW = DatePart(WeekDay,@dToday)
if @iDow = 1 
Set @iDow = 8
Select @dCycle = DateAdd(DD,2-@iDoW,@dToday)
Select @dCycleMinus8 = DateAdd(DD,-56,@dCycle)
Select @dCycleMinus52 = DateAdd(DD,-364,@dCycle)

-- Add this weeks data to History
Delete from   [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_PRIORITY_HISTORY Where CYCLE = @dCycle
Insert   INTO [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_PRIORITY_HISTORY
Select * FROM [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_PRIORITY Where CYCLE = @dCycle

Delete from   [SC_PLN_DS].[SUPPLY_COMMIT].LATENESS_RAW_HISTORY Where CYCLE = @dCycle
Insert   INTO [SC_PLN_DS].[SUPPLY_COMMIT].LATENESS_RAW_HISTORY
Select * FROM [SC_PLN_DS].[SUPPLY_COMMIT].LATENESS_RAW Where CYCLE = @dCycle

Delete FROM   [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_SUPPORTABILITY_HISTORY Where CYCLE = @dCycle
Insert   INTO [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_SUPPORTABILITY_HISTORY
Select * FROM [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_SUPPORTABILITY Where CYCLE = @dCycle

-- Remove Anything too Old from Current
delete from Supply_Commit.Demand_Priority where CYCLE <= @dCycleMinus8
delete from Supply_Commit.Demand_Supportability where CYCLE <= @dCycleMinus8
delete from Supply_Commit.Lateness_Raw where CYCLE <= @dCycleMinus8

-- delete really old stuff from History
delete from Supply_Commit.Demand_Priority_History where CYCLE <= @dCycleMinus52
delete from Supply_Commit.Lateness_Raw_History where CYCLE <= @dCycleMinus52
delete from Supply_Commit.Demand_Supportability_History where CYCLE <= @dCycleMinus52

