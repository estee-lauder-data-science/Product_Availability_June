delete
  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[RESULT]
  where CYCLE = '2022-01-17'

delete
  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[STG_PBI_RESULT]
  where CYCLE = '2022-01-17'

  delete
  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[PBI_RESULT]
  where CYCLE = '2022-01-17'


    delete
  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[RCA]
  where CYCLE = '2022-01-17'
