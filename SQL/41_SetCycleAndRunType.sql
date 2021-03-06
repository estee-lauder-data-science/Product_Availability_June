-- Summary: updates Cycle and RunType in the 3 main tables
-- Determine Cycle and Run_Type
DECLARE @dCycle AS DATE;
DECLARE @sRunType as varchar(10)
Set @dCycle = dbo.DSGetPriorMonday(getDate())
Set @sRunType = dbo.DSGetCDPRunType(getDate())

-- MAIN
UPDATE [SUPPLY_COMMIT].[LATENESS_RAW]          SET CYCLE = @dCycle, RUN_TYPE = @sRunType WHERE CYCLE IS NULL;
UPDATE [SUPPLY_COMMIT].[DEMAND_PRIORITY]       SET CYCLE = @dCycle, RUN_TYPE = @sRunType WHERE CYCLE IS NULL;
UPDATE [SUPPLY_COMMIT].[DEMAND_SUPPORTABILITY] SET CYCLE = @dCycle, RUN_TYPE = @sRunType WHERE CYCLE IS NULL;
