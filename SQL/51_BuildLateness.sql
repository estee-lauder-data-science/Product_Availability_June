DROP TABLE IF EXISTS [SUPPLY_COMMIT].[LATENESS];

SELECT

[Material]
,[Region]
,[Market]
,[Location]
,[Fiscal Week]
,COALESCE(SUM(ONE_MNTH_LATE), 0) AS ONE_MNTH_LATE
,COALESCE(SUM(TWO_MNTHS_LATE), 0) AS TWO_MNTHS_LATE
,COALESCE(SUM(THREE_MNTHS_LATE), 0) AS THREE_MNTHS_LATE
,COALESCE(SUM(ONE_QTR_LATE), 0) AS ONE_QTR_LATE

,COALESCE(SUM(IN_CLNDR_MNTH_LATE), 0) AS IN_CLNDR_MNTH_LATE
,COALESCE(SUM(IN_FSCL_QTR_LATE), 0) AS IN_FSCL_QTR_LATE
,COALESCE(SUM(OUTSIDE_FSCL_QTR_LATE), 0) AS OUTSIDE_FSCL_QTR_LATE

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

,CASE
	WHEN LATE_FISCAL_BUCKET = 'IN CALENDAR MONTH' THEN LATE
ELSE 0 END AS IN_CLNDR_MNTH_LATE
,CASE
	WHEN LATE_FISCAL_BUCKET = 'IN FISCAL QUARTER' THEN LATE
ELSE 0 END AS IN_FSCL_QTR_LATE
,CASE
	WHEN LATE_FISCAL_BUCKET = 'OUTSIDE FISCAL QUARTER' THEN LATE
ELSE 0 END AS OUTSIDE_FSCL_QTR_LATE

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
,LATE_FISCAL_BUCKET

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
,LATE_FISCAL_BUCKET

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
				,CASE
                        		WHEN MONTH(LATE_WEEK_DATE) = MONTH(DEMAND_WEEK_DATE) AND YEAR(LATE_WEEK_DATE) = YEAR(DEMAND_WEEK_DATE) THEN 'IN CALENDAR MONTH'
                        		WHEN LATE_FISCAL_QUARTER = DEMAND_FISCAL_QUARTER AND YEAR(LATE_WEEK_DATE) = YEAR(DEMAND_WEEK_DATE) THEN 'IN FISCAL QUARTER'
                        		ELSE 'OUTSIDE FISCAL QUARTER'
                		END LATE_FISCAL_BUCKET

		
		FROM (
		
		SELECT

		a.*,
		CASE
			WHEN MONTH(LATE_WEEK_DATE) IN (7, 8, 9) THEN 'Q1'
			WHEN MONTH(LATE_WEEK_DATE) IN (10, 11, 12) THEN 'Q2'
			WHEN MONTH(LATE_WEEK_DATE) IN (1, 2, 3) THEN 'Q3'
			WHEN MONTH(LATE_WEEK_DATE) IN (4, 5, 6) THEN 'Q4'
		END AS LATE_FISCAL_QUARTER,
		CASE
			WHEN MONTH(DEMAND_WEEK_DATE) IN (7, 8, 9) THEN 'Q1'
			WHEN MONTH(DEMAND_WEEK_DATE) IN (10, 11, 12) THEN 'Q2'
			WHEN MONTH(DEMAND_WEEK_DATE) IN (1, 2, 3) THEN 'Q3'
			WHEN MONTH(DEMAND_WEEK_DATE) IN (4, 5, 6) THEN 'Q4'
		END AS DEMAND_FISCAL_QUARTER

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
						  ,c.CALENDAR_WEEK AS LATE_WEEK_DATE
						  ,b.CALENDAR_WEEK AS DEMAND_WEEK_DATE

					  FROM [SC_PLN_DS].[SUPPLY_COMMIT].[LATENESS_RAW] a 
					  LEFT JOIN [MASTER_DATA].[WEEKLY_FISCAL_CALENDAR] b ON a.[Fiscal Week] = b.ISO_WEEK
					  LEFT JOIN [MASTER_DATA].[WEEKLY_FISCAL_CALENDAR] c ON a.[Late Fiscal Week] = c.ISO_WEEK

					  WHERE a.[Region] NOT IN ('ArtificialFlowDemand', 'Estimate')
					  --AND a.Material = '7T5R010000'

			) a

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
,LATE_FISCAL_BUCKET

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

