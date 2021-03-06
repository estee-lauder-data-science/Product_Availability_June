DROP TABLE IF EXISTS [SUPPLY_COMMIT].[LATENESS];

SELECT

[Material]
,[Region]
,[Market]
,[Location]
,[Fiscal Week]
,SUM(ONE_MNTH_LATE) AS ONE_MNTH_LATE
,SUM(TWO_MNTHS_LATE) AS TWO_MNTHS_LATE
,SUM(THREE_MNTHS_LATE) AS THREE_MNTHS_LATE
,SUM(ONE_QTR_LATE) AS ONE_QTR_LATE
,CYCLE
,RUN_TYPE
,getDate() as archLoadDate
INTO [SUPPLY_COMMIT].[LATENESS]
FROM (

SELECT

[Material]
,[Region]
,[Market]
,[Location]
,[Fiscal Week]
,CASE
	WHEN LATE_BUCKET = '< 1 MONTH' THEN LATE
ELSE 0 END AS ONE_MNTH_LATE
,CASE
	WHEN LATE_BUCKET = '< 2 MONTHS' THEN LATE
ELSE 0 END AS TWO_MNTHS_LATE
,CASE
	WHEN LATE_BUCKET = '< 3 MONTHS' THEN LATE
ELSE 0 END AS THREE_MNTHS_LATE
,CASE
	WHEN LATE_BUCKET = '> 3 MONTHS' THEN LATE
ELSE 0 END AS ONE_QTR_LATE
,CYCLE
,RUN_TYPE

FROM (

SELECT

[Material]
,[Region]
,[Market]
,[Location]
,[Fiscal Week]
,SUM([Late Qty]) AS LATE
,LATE_BUCKET
,CYCLE
,RUN_TYPE

FROM (

SELECT

[Material]
,[Region]
,[Location]
,[Market]
,[Fiscal Week]
,[Late Qty]
,CASE 
	WHEN WEEKS_LATE < 5 THEN '< 1 MONTH'
	WHEN WEEKS_LATE < 9 THEN '< 2 MONTHS'
	WHEN WEEKS_LATE < 13 THEN '< 3 MONTHS'
	ELSE '> 3 MONTHS'
END AS LATE_BUCKET
,CYCLE
,RUN_TYPE

FROM (

		SELECT   a.[Material]
				,a.[Region]
				,a.[Location]
				,a.[Market]
				,a.[Fiscal Week]
				--,a.[Late Fiscal Week]
				,a.[Late Qty]
				,a.[Demand Qty]
				,CASE
					WHEN [Fiscal Year] = [Late Fiscal Year] THEN LATE_WEEK - DEMAND_WEEK
					WHEN [Fiscal Year] + 1 = [Late Fiscal Year] THEN LATE_WEEK + (CASE WHEN [Fiscal Year] = 2020 THEN 53 ELSE 52 END - DEMAND_WEEK)
				END AS WEEKS_LATE
				,a.CYCLE
				,a.RUN_TYPE

		FROM (
		
					SELECT a.[Material]
						  ,a.[Region]
						  ,a.[Location]
						  ,a.[Market]
						  ,a.[Fiscal Week]
						  --,a.[Late Fiscal Week] --EXCL
						  ,RIGHT(a.[Fiscal Week], 4) AS [Fiscal Year]
						  ,RIGHT(a.[Late Fiscal Week], 4) AS [Late Fiscal Year]
						  ,a.[Late Qty]
						  ,a.[Demand Qty]
						  ,TRY_CAST(RIGHT(LEFT(a.[Late Fiscal Week], 3) , 2) AS INT) AS LATE_WEEK
						  ,TRY_CAST(RIGHT(LEFT(a.[Fiscal Week], 3) , 2) AS INT) AS DEMAND_WEEK
						  ,a.CYCLE
						  ,a.RUN_TYPE

					  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[LATENESS_RAW] a 

					  WHERE a.[Region] NOT IN ('ArtificialFlowDemand', 'Estimate')
					  --AND a.Material = 'PF79466000'

			) a

  ) a

  ) a

  GROUP BY

 [Material]
,[Region]
,[Location]
,[Market]
,[Fiscal Week]
,LATE_BUCKET
,CYCLE
,RUN_TYPE

) a

) a

GROUP BY

[Material]
,[Region]
,[Market]
,[Location]
,[Fiscal Week]
,CYCLE
,RUN_TYPE
;

