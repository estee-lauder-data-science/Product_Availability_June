/****** Script for SelectTopNRows command from SSMS  ******/
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_Priority  where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].LATENESS_RAW where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].DEMAND_SUPPORTABILITY  where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].RESULT  where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].[STG_PBI_RESULT] where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].LATENESS where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].RCA where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].PBI_RESULT where  CYCLE = '2022-05-30'
delete FROM [SC_PLN_DS].[SUPPLY_COMMIT].RISKLESS_FLAG where  CYCLE = '2022-05-30'


