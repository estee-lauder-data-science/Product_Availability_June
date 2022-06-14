
DECLARE @RUN_TYPE AS VARCHAR(10);
DECLARE @WEEK as Date;
Set @RUN_TYPE = dbo.DSGetCDPRunType(getDate());
Set @WEEK = dbo.DSGetPriorMonday(getDate());


SELECT [SKU_10D]
      ,[LOCATION]
      ,[FISCAL_MONTH]
      ,[FISCAL_YEAR]
      ,[MARKET]
      ,[RUN_TYPE]
      ,[CYCLE]
      ,[DEMAND_PRIORITY_12D]

      ,SUM([MATERIAL_CAPACITY_CONSTRAINTS_DOLLARS]) AS MATERIAL_CAPACITY_CONSTRAINTS_DOLLARS
      ,SUM([MATERIAL_LEADTIME_CONSTRAINTS_DOLLARS]) AS MATERIAL_LEADTIME_CONSTRAINTS_DOLLARS
      ,SUM([MATERIAL_BROKEN_NETWORK_CONSTRAINTS_DOLLARS]) AS MATERIAL_BROKEN_NETWORK_CONSTRAINTS_DOLLARS
      ,SUM([MASS_CAPACITY_SHORTAGE_DOLLARS]) AS MASS_CAPACITY_SHORTAGE_DOLLARS
      ,SUM([FPA_CAPACITY_SHORTAGE_DOLLARS]) AS FPA_CAPACITY_SHORTAGE_DOLLARS
      ,SUM([FROZEN_WINDOW_CONSTRAINTS_DOLLARS]) AS FROZEN_WINDOW_CONSTRAINTS_DOLLARS
      ,SUM([LEADTIME_BEFORE_CURRENT_CONSTRAINTS_DOLLARS]) AS LEADTIME_BEFORE_CURRENT_CONSTRAINTS_DOLLARS
      ,SUM([PLANNING_CALENDAR_CONSTRAINTS_DOLLARS]) AS PLANNING_CALENDAR_CONSTRAINTS_DOLLARS
      ,SUM([EFFECTIVITY_CONSTRAINTS_DOLLARS]) AS EFFECTIVITY_CONSTRAINTS_DOLLARS
      ,SUM([BROKEN_NETWORK_CONSTRAINTS_DOLLARS]) AS BROKEN_NETWORK_CONSTRAINTS_DOLLARS

  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[STG_PBI_RESULT]
  WHERE [CYCLE] = @WEEK
  AND RUN_TYPE = @RUN_TYPE


GROUP BY

[SKU_10D]
      ,[LOCATION]
      ,[FISCAL_MONTH]
      ,[FISCAL_YEAR]
      ,[MARKET]
      ,[RUN_TYPE]
      ,[CYCLE]
      ,[DEMAND_PRIORITY_12D]