

DECLARE @RUN_TYPE AS VARCHAR(10);
DECLARE @WEEK as Date;
Set @RUN_TYPE = dbo.DSGetCDPRunType(getDate());
Set @WEEK = dbo.DSGetPriorMonday(getDate());

			

INSERT INTO [SC_PLN_DS].[SUPPLY_COMMIT].[RESULT]

SELECT     a.*
  ,1.00*a.MATERIAL_CAPACITY_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS MATERIAL_CAPACITY_CONSTRAINTS_DOLLARS		
  ,1.00*a.MATERIAL_LEADTIME_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS MATERIAL_LEADTIME_CONSTRAINTS_DOLLARS
  ,1.00*a.MATERIAL_BROKEN_NETWORK_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS MATERIAL_BROKEN_NETWORK_CONSTRAINTS_DOLLARS 
  ,1.00*a.MASS_CAPACITY_SHORTAGE*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS MASS_CAPACITY_SHORTAGE_DOLLARS
  ,1.00*a.FPA_CAPACITY_SHORTAGE*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS FPA_CAPACITY_SHORTAGE_DOLLARS
  ,1.00*a.FROZEN_WINDOW_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS FROZEN_WINDOW_CONSTRAINTS_DOLLARS	
  ,1.00*a.LEADTIME_BEFORE_CURRENT_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS LEADTIME_BEFORE_CURRENT_CONSTRAINTS_DOLLARS 
  ,1.00*a.PLANNING_CALENDAR_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS PLANNING_CALENDAR_CONSTRAINTS_DOLLARS	 
  ,1.00*a.EFFECTIVITY_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS EFFECTIVITY_CONSTRAINTS_DOLLARS 
  ,1.00*a.BROKEN_NETWORK_CONSTRAINTS*COALESCE(a.DEMAND_QTY_EX/NULLIF(a.DEMAND_QTY_PROPORTION, 0), 0) AS BROKEN_NETWORK_CONSTRAINTS_DOLLARS

FROM (

    SELECT [SOURCE_PLANT]
      ,[SOURCE_PLANT_NAME]
      ,[SOURCE_PLANT_MATERIAL_STATUS]
      ,[ITEM_PRIORITY]
      ,[SKU_10D]
      ,[SKU_6D]
      ,[ITEM_DESCRIPTION]
      ,[SKU_4D]
      ,[BRAND]
      ,[MAJOR_CATEGORY]
      ,[CATEGORY_DESCRIPTION]
      ,[MAJOR_INVENTORY_DESCRIPTION]
      ,[ITEM_GROUP]
      ,[INVENTORY_DESCRIPTION]
      ,[LAUNCH_BASIC]
      ,[SUPPLY_DEMAND_TYPE]
      ,[GLOBAL_ABCD]
      ,[LOCAL_ABCD]
      ,[HERO]
      ,a.[REGION]
      ,[SAP_REGION]
      ,a.[LOCATION]
      ,[LOCATION_DESCRIPTION]
      --,[FISCAL_WEEK]
      ,[FISCAL_MONTH_YEAR]
      ,[FISCAL_MONTH]
      ,[FISCAL_YEAR]
      ,[MONTH]
      ,[QUARTER]
      ,[INVENTORY_TYPE]
      ,a.[MARKET]
      ,a.[RUN_TYPE]
      ,a.[SET_INDICATOR]
      ,SUM([LATE_QTY_PROPORTION]) AS LATE_QTY_PROPORTION
      ,SUM([LATE_QTY_EX]) AS LATE_QTY_EX
      ,SUM([DEMAND_QTY_PROPORTION]) AS DEMAND_QTY_PROPORTION
      ,SUM([DEMAND_QTY_EX]) AS DEMAND_QTY_EX
      ,SUM([ON_TIME_QTY_PROPORTION]) AS ON_TIME_QTY_PROPORTION
      ,SUM([ON_TIME_QTY_EX]) AS ON_TIME_QTY_EX
      ,SUM([UNSUPPORTABILITY_PROPORTION]) AS UNSUPPORTABILITY_PROPORTION
      ,SUM([UNSUPPORTABILITY_PROPORTION_EX]) AS UNSUPPORTABILITY_PROPORTION_EX
      ,a.[CYCLE]
      ,[FRANCHISE]
      ,[SUB_CATEGORY]
      ,[APPLICATION]
      ,[RISKLESS_FLAG]
      ,[DEMAND_PRIORITY_12D]

      ,SUM(COALESCE(f.ONE_MNTH_LATE, 0)) AS ONE_MNTH_LATE
      ,SUM(COALESCE(f.TWO_MNTHS_LATE, 0)) AS TWO_MNTHS_LATE
      ,SUM(COALESCE(f.THREE_MNTHS_LATE, 0)) AS THREE_MNTHS_LATE
      ,SUM(COALESCE(f.ONE_QTR_LATE, 0)) AS ONE_QTR_LATE

      ,SUM(COALESCE(f.ONE_MNTH_LATE*a.SELL_IN_PRICE, 0)) AS ONE_MNTH_LATE_DOLLARS
      ,SUM(COALESCE(f.TWO_MNTHS_LATE*a.SELL_IN_PRICE, 0)) AS TWO_MNTHS_LATE_DOLLARS
      ,SUM(COALESCE(f.THREE_MNTHS_LATE*a.SELL_IN_PRICE, 0)) AS THREE_MNTHS_LATE_DOLLARS
      ,SUM(COALESCE(f.ONE_QTR_LATE*a.SELL_IN_PRICE, 0)) AS ONE_QTR_LATE_DOLLARS

	  ,SUM(COALESCE(f.IN_CLNDR_MNTH_LATE, 0)) AS IN_CLNDR_MNTH_LATE
      ,SUM(COALESCE(f.IN_FSCL_QTR_LATE, 0)) AS IN_FSCL_QTR_LATE
      ,SUM(COALESCE(f.OUTSIDE_FSCL_QTR_LATE, 0)) AS OUTSIDE_FSCL_QTR_LATE

      ,SUM(COALESCE(f.IN_CLNDR_MNTH_LATE*a.SELL_IN_PRICE, 0)) AS IN_CLNDR_MNTH_LATE_DOLLARS
      ,SUM(COALESCE(f.IN_FSCL_QTR_LATE*a.SELL_IN_PRICE, 0)) AS IN_FSCL_QTR_LATE_DOLLARS
      ,SUM(COALESCE(f.OUTSIDE_FSCL_QTR_LATE*a.SELL_IN_PRICE, 0)) AS OUTSIDE_FSCL_QTR_LATE_DOLLARS
      
      ,SUM(a.MATERIAL_CAPACITY_CONSTRAINTS) AS MATERIAL_CAPACITY_CONSTRAINTS		
      ,SUM(a.MATERIAL_LEADTIME_CONSTRAINTS) AS MATERIAL_LEADTIME_CONSTRAINTS
      ,SUM(a.MATERIAL_BROKEN_NETWORK_CONSTRAINTS) AS MATERIAL_BROKEN_NETWORK_CONSTRAINTS
      ,SUM(a.MASS_CAPACITY_SHORTAGE) AS MASS_CAPACITY_SHORTAGE	
      ,SUM(a.FPA_CAPACITY_SHORTAGE) AS FPA_CAPACITY_SHORTAGE
      ,SUM(a.FROZEN_WINDOW_CONSTRAINTS) AS FROZEN_WINDOW_CONSTRAINTS	
      ,SUM(a.LEADTIME_BEFORE_CURRENT_CONSTRAINTS) AS LEADTIME_BEFORE_CURRENT_CONSTRAINTS
      ,SUM(a.PLANNING_CALENDAR_CONSTRAINTS) AS PLANNING_CALENDAR_CONSTRAINTS	 
      ,SUM(a.EFFECTIVITY_CONSTRAINTS) AS EFFECTIVITY_CONSTRAINTS 
      ,SUM(a.BROKEN_NETWORK_CONSTRAINTS) AS BROKEN_NETWORK_CONSTRAINTS			
			
	FROM
        (
			
            SELECT a.[Source Plant] AS SOURCE_PLANT
              ,a.[Source Plant Name] AS SOURCE_PLANT_NAME
              ,a.[Source Plant Material Status] AS SOURCE_PLANT_MATERIAL_STATUS
              ,a.[Item Priority] AS ITEM_PRIORITY
              ,a.[Material] AS SKU_10D
              ,a.[Material_6D] AS SKU_6D
              ,a.[Material Description] AS ITEM_DESCRIPTION
              ,a.[Family Code] AS SKU_4D
              ,a.[Brand] AS BRAND
              ,a.[Major Category] AS MAJOR_CATEGORY
              ,a.[Category Description] AS CATEGORY_DESCRIPTION
              ,a.[Major Inventory Description] AS MAJOR_INVENTORY_DESCRIPTION
              ,a.[Item Group] AS ITEM_GROUP
              ,a.[Inventory Description] AS INVENTORY_DESCRIPTION
              ,a.[Launch Basic] AS LAUNCH_BASIC
              ,a.[Supply Demand Type] AS SUPPLY_DEMAND_TYPE
              ,a.[Global ABCD] AS GLOBAL_ABCD
              ,a.[Local ABCD] AS LOCAL_ABCD
              ,a.[Hero] AS HERO
              ,CASE
                            WHEN a.[Region] = 'APC' THEN 'DP_APAC'
                            WHEN a.[Region] = 'EMA' THEN 'DP_EMEA'
                            WHEN a.[Region] = 'LAT' THEN 'DP_LATINA'
                            WHEN a.[Region] = 'TRD' THEN 'DP_TR'
                            WHEN a.[Region] = 'NAM' THEN 'DP_NA'
                            WHEN a.[Region] = 'UKD' THEN 'DP_UK'
							WHEN a.[Region] = 'CHN' THEN 'DP_CHINA'
                            ELSE a.[Region]
               END AS REGION
              ,a.[SAP_REGION] AS SAP_REGION
              ,a.[Location] AS LOCATION
              ,a.[Location Desc] AS LOCATION_DESCRIPTION
              ,a.[Fiscal Week] AS FISCAL_WEEK 
              ,a.[Fiscal Month] AS FISCAL_MONTH_YEAR
              ,a.[Fiscal_Month] AS FISCAL_MONTH
              ,a.[Fiscal Year] AS FISCAL_YEAR
              ,a.[Month] AS MONTH
              ,a.[Quarter] AS QUARTER
              ,a.[INVTYPE] AS INVENTORY_TYPE
              ,a.[Market] AS MARKET
              ,a.RUN_TYPE
              ,COALESCE(e.SET_INDICATOR, 'N') AS SET_INDICATOR
              ,SUM(a.[Late Qty Proportion]) AS LATE_QTY_PROPORTION
              ,SUM(CASE
                                    WHEN e.SET_INDICATOR = 'GWP' AND a.BRAND = f.BRAND THEN a.[Late Qty Proportion]*f.PRICE_PER_UNIT
                                    ELSE a.[LATE QTY EX]
                       END
                      ) AS LATE_QTY_EX
              ,SUM(a.[Demand Qty Proportion]) AS DEMAND_QTY_PROPORTION
              ,SUM(
                            CASE
                                    WHEN e.SET_INDICATOR = 'GWP' AND a.BRAND = f.BRAND THEN a.[Demand Qty Proportion]*f.PRICE_PER_UNIT
                                    ELSE a.[Demand Qty EX]
                       END
                      ) AS DEMAND_QTY_EX
              ,1.00*SUM(
                                    CASE
                                            WHEN e.SET_INDICATOR = 'GWP' AND a.BRAND = f.BRAND THEN a.[Demand Qty Proportion]*f.PRICE_PER_UNIT
                                            ELSE a.[Demand Qty EX]
                                    END
                               )/NULLIF(SUM(a.[Demand Qty Proportion]), 0) AS SELL_IN_PRICE
              ,SUM(a.[On Time Quantity Proportion]) AS ON_TIME_QTY_PROPORTION
              ,SUM(
                            CASE
                                    WHEN e.SET_INDICATOR = 'GWP' AND a.BRAND = f.BRAND THEN a.[On Time Quantity Proportion]*f.PRICE_PER_UNIT
                                    ELSE a.[On Time Quantity EX]
                            END
                      ) AS ON_TIME_QTY_EX
              ,SUM(a.[Un-Supportability Proportion]) AS UNSUPPORTABILITY_PROPORTION
              ,SUM(
                            CASE
                                    WHEN e.SET_INDICATOR = 'GWP' AND a.BRAND = f.BRAND THEN a.[Un-Supportability Proportion]*f.PRICE_PER_UNIT
                                    ELSE a.[Un-Supportability EX]
                            END
                      ) AS UNSUPPORTABILITY_PROPORTION_EX 
              ,a.CYCLE
              ,MIN(c.[PRODUCT_LINE_DESCRIPTION]) AS FRANCHISE
              ,MIN(c.[SUB_CATEGORY_DESCRIPTION]) AS SUB_CATEGORY
              ,MIN(c.[APPLICATION_DESCRIPTION]) AS APPLICATION
              ,d.RISKLESS_FLAG

              ,SUM(a.[Material Capacity Constraints]) AS MATERIAL_CAPACITY_CONSTRAINTS		
              ,SUM(a.[Material Leadtime Constraints]) AS MATERIAL_LEADTIME_CONSTRAINTS
              ,SUM(a.[Material Broken Network Constraints]) AS MATERIAL_BROKEN_NETWORK_CONSTRAINTS
              ,SUM(a.[Mass Capacity Shortage]) AS MASS_CAPACITY_SHORTAGE	
              ,SUM(a.[F&A Capacity Shortage]) AS FPA_CAPACITY_SHORTAGE
              ,SUM(a.[Frozen Window Constrants]) AS FROZEN_WINDOW_CONSTRAINTS	
              ,SUM(a.[LeadTime Before Current Constraints]) AS LEADTIME_BEFORE_CURRENT_CONSTRAINTS
              ,SUM(a.[Planning Calendar Constraints]) AS PLANNING_CALENDAR_CONSTRAINTS	 
              ,SUM(a.[Effectivity Constraints]) AS EFFECTIVITY_CONSTRAINTS 
              ,SUM(a.[Broken Network Constraints]) AS BROKEN_NETWORK_CONSTRAINTS

              ,k.DEMAND_PRIORITY AS DEMAND_PRIORITY_12D

		FROM (
			  
                  SELECT a.* FROM [SC_PLN_DS].[SUPPLY_COMMIT].[DEMAND_SUPPORTABILITY] a
                  WHERE a.CYCLE = @WEEK AND a.RUN_TYPE = @RUN_TYPE
                  ) a
            
                  LEFT JOIN [SC_PLN_DS].[MASTER_DATA].ITEM_SAP_CATEGORIZATION c ON a.[Material] = c.[ITEM_ID_9_0]
                  LEFT JOIN [SC_PLN_DS].[SUPPLY_COMMIT].[RISKLESS_FLAG] d ON a.[Material] = d.SKU_10D AND a.MARKET = d.MARKET AND a.CYCLE = d.CYCLE AND a.RUN_TYPE = d.RUN_TYPE
                  LEFT JOIN [SC_PLN_DS].[MASTER_DATA].[SETS] e ON a.Material = e.ITEM_ID_9_0	
                  LEFT JOIN [SC_PLN_DS].[MASTER_DATA].[GWP_PRICE_MATRIX] f ON a.Brand = f.BRAND
                  LEFT JOIN (
                  
                  SELECT DISTINCT
                                 [SCSItem  Item ] AS SKU_10D
                                 ,[SCSLocation  Location ] AS LOCATION
                                 ,[SalesAccount  ReportingCustomer ] AS MARKET
                                 ,[Time  FiscalWeek ] AS FISCAL_WEEK
                                 --,[SCSDemand  DemandID ] AS DEMAND_ID
                                 --,[SCSDemandType  DemandType ] AS DEMAND_TYPE
                                 ,MIN([SCPDemandPriorityGDPForUI]) AS DEMAND_PRIORITY
                                 ,[CYCLE]
                                 ,[RUN_TYPE]
                  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[DEMAND_PRIORITY]

			  GROUP BY

			     [SCSItem  Item ]
				,[SCSLocation  Location ]
				,[SalesAccount  ReportingCustomer ]
				,[Time  FiscalWeek ]
				,[CYCLE]
				,[RUN_TYPE]
			  
			  ) k ON a.Material = k.SKU_10D AND a.Location = k.LOCATION AND a.Market = k.MARKET AND a.[Fiscal Week] = k.FISCAL_WEEK AND a.RUN_TYPE = k.RUN_TYPE AND a.CYCLE = k.CYCLE  

			  GROUP BY

                           a.[Source Plant]
                          ,a.[Source Plant Name]
                          ,a.[Source Plant Material Status]
                          ,a.[Item Priority]
                          ,a.[Material]
                          ,a.[Material_6D]
                          ,a.[Material Description]
                          ,a.[Family Code]
                          ,a.[Brand]
                          ,a.[Major Category]
                          ,a.[Category Description]
                          ,a.[Major Inventory Description]
                          ,a.[Item Group]
                          ,a.[Inventory Description]
                          ,a.[Launch Basic]
                          ,a.[Supply Demand Type]
                          ,a.[Global ABCD]
                          ,a.[Local ABCD]
                          ,a.[Hero]
                          ,a.[Region]
                          ,a.[SAP_REGION]
                          ,a.[Location]
                          ,a.[Location Desc]
                          ,a.[Fiscal Week]
                          ,a.[Fiscal Month]
                          ,a.[Fiscal_Month]
                          ,a.[Fiscal Year]
                          ,a.[Month]
                          ,a.[Quarter]    
                          ,a.[INVTYPE]  
                          ,a.CYCLE
                          ,a.[Market]
                          ,d.RISKLESS_FLAG
                          ,e.SET_INDICATOR
                          ,a.RUN_TYPE
                          ,k.DEMAND_PRIORITY

                          ) a
			LEFT JOIN [SC_PLN_DS].[SUPPLY_COMMIT].[LATENESS] f ON a.[SKU_10D] = f.Material AND a.Market = f.Market AND a.Location = f.Location AND a.FISCAL_WEEK = f.[Fiscal Week] AND a.Region = f.Region AND a.CYCLE = f.CYCLE AND a.RUN_TYPE = f.RUN_TYPE

        GROUP BY

	   [SOURCE_PLANT]
      ,[SOURCE_PLANT_NAME]
      ,[SOURCE_PLANT_MATERIAL_STATUS]
      ,[ITEM_PRIORITY]
      ,[SKU_10D]
      ,[SKU_6D]
      ,[ITEM_DESCRIPTION]
      ,[SKU_4D]
      ,[BRAND]
      ,[MAJOR_CATEGORY]
      ,[CATEGORY_DESCRIPTION]
      ,[MAJOR_INVENTORY_DESCRIPTION]
      ,[ITEM_GROUP]
      ,[INVENTORY_DESCRIPTION]
      ,[LAUNCH_BASIC]
      ,[SUPPLY_DEMAND_TYPE]
      ,[GLOBAL_ABCD]
      ,[LOCAL_ABCD]
      ,[HERO]
      ,a.[REGION]
      ,[SAP_REGION]
      ,a.[LOCATION]
      ,[LOCATION_DESCRIPTION]
      --,[FISCAL_WEEK]
      ,[FISCAL_MONTH_YEAR]
      ,[FISCAL_MONTH]
      ,[FISCAL_YEAR]
      ,[MONTH]
      ,[QUARTER]
      ,[INVENTORY_TYPE]
      ,a.[MARKET]
      ,a.[RUN_TYPE]
      ,a.[SET_INDICATOR]
      ,a.[CYCLE]
      ,[FRANCHISE]
      ,[SUB_CATEGORY]
      ,[APPLICATION]
      ,[RISKLESS_FLAG]
	  ,[DEMAND_PRIORITY_12D]

) a
    LEFT JOIN [SC_PLN_DS].[MASTER_DATA].[ITEM_MASTER] b ON a.[SKU_10D] = b.SAP_ITEM_ID AND a.LOCATION = b.SAP_NODE
;


INSERT INTO [SC_PLN_DS].[SUPPLY_COMMIT].[STG_PBI_RESULT]

SELECT
a.*,
COALESCE(b.CAL_DEMAND_TYPE, '') AS DEMAND_TYPE,
COALESCE(c.PTPM, 'N') AS PTPM

--INTO [SC_PLN_DS].[SUPPLY_COMMIT].[PBI_RESULT]
FROM [SC_PLN_DS].[SUPPLY_COMMIT].[RESULT] a
LEFT JOIN 
(
SELECT DISTINCT SAP_ITEM_ID, CAL_DEMAND_TYPE
FROM [SC_PLN_DS].[MASTER_DATA].[ITEM_MASTER]
) b ON a.SKU_10D = b.SAP_ITEM_ID
LEFT JOIN
(
SELECT SAP_ITEM_ID, SAP_NODE, 'Y' AS PTPM
FROM [SC_PLN_DS].[MASTER_DATA].[ITEM_MASTER]
WHERE MATL_TYPE IN ('FERT', 'HALB')
AND PROC_TYPE = 'F'
AND SPT IN ('30', '50')
AND SAP_NODE IN ('1010', '2010', '2020', '4010', '4020', '6010', '6020', '6030')
) c  ON a.SKU_10D = c.SAP_ITEM_ID AND a.SOURCE_PLANT = c.SAP_NODE  
WHERE a.CYCLE = @WEEK AND a.RUN_TYPE = @RUN_TYPE
;

