-- This is only run after we are sure that Demand_Supportability has been fully copied to Demand_Supportability_Temp
--drop table Supply_Commit.Demand_Supportability_Matt
-- STart: 
-- DS_Matt: Full, no cycle but idex
-- DS_Temp  Full, with Cycle but no index

-- 1) Backup main table; rename _Temp to Main; apply index to main; recreate _Temp
drop table if exists [SUPPLY_COMMIT].Demand_Supportability_Backup
go
EXEC sp_rename '[Supply_Commit].Demand_Supportability', 'Demand_Supportability_Backup';
go
EXEC sp_rename '[Supply_Commit].Demand_Supportability_Temp', 'Demand_Supportability';
go 

CREATE CLUSTERED INDEX IDX_CYCL_MKT_MTL_RSC ON [SUPPLY_COMMIT].[DEMAND_SUPPORTABILITY] (
CYCLE ASC,
RUN_TYPE ASC,
Region ASC,
Location ASC,
Brand ASC,
Market ASC,
Affiliate ASC,
Material ASC,
[Fiscal Week] ASC,
[Fiscal Year] ASC
)
;

drop table if exists [SUPPLY_COMMIT].[Demand_Supportability_Temp]
go
CREATE TABLE [SUPPLY_COMMIT].[Demand_Supportability_Temp](
	[Source Plant] [varchar](50) NULL,
	[Source Plant Name] [varchar](50) NULL,
	[Source Plant Material Status] [varchar](50) NULL,
	[Item Priority] [varchar](50) NULL,
	[Material] [varchar](50) NULL,
	[Material_6D] [varchar](50) NULL,
	[Material Description] [varchar](200) NULL,
	[Family Code] [varchar](50) NULL,
	[Brand] [varchar](50) NULL,
	[Major Category] [varchar](50) NULL,
	[Category Description] [varchar](50) NULL,
	[Major Inventory Description] [varchar](50) NULL,
	[Item Group] [varchar](50) NULL,
	[Inventory Description] [varchar](50) NULL,
	[Launch Basic] [varchar](50) NULL,
	[Supply Demand Type] [varchar](50) NULL,
	[Global ABCD] [varchar](50) NULL,
	[Local ABCD] [varchar](50) NULL,
	[Hero] [varchar](50) NULL,
	[Region] [varchar](50) NULL,
	[SAP_REGION] [varchar](50) NULL,
	[Affiliate] [varchar](50) NULL,
	[Location] [varchar](50) NULL,
	[Location Desc] [varchar](50) NULL,
	[Market] [varchar](50) NULL,
	[Fiscal Week] [varchar](50) NULL,
	[Fiscal Month] [varchar](50) NULL,
	[Fiscal_Month] [varchar](50) NULL,
	[Fiscal Year] [varchar](50) NULL,
	[Month] [varchar](50) NULL,
	[Quarter] [varchar](50) NULL,
	[Resource Classification] [varchar](1000) NULL,
	[Material Capacity Constraints] [real] NULL,
	[Material Leadtime Constraints] [real] NULL,
	[Material Broken Network Constraints] [real] NULL,
	[Mass Capacity Shortage] [real] NULL,
	[F&A Capacity Shortage] [real] NULL,
	[Frozen Window Constrants] [real] NULL,
	[LeadTime Before Current Constraints] [real] NULL,
	[Planning Calendar Constraints] [real] NULL,
	[Effectivity Constraints] [real] NULL,
	[Broken Network Constraints] [real] NULL,
	[Review Flag] [varchar](50) NULL,
	[MM To Action] [varchar](50) NULL,
	[MM Reviewed] [varchar](50) NULL,
	[Mass to Action] [varchar](50) NULL,
	[Mass Reviewed] [varchar](50) NULL,
	[INVTYPE] [varchar](50) NULL,
	[CHANNEL] [varchar](50) NULL,
	[CHANNEL_NAME] [varchar](50) NULL,
	[KEYACCOUNT] [varchar](50) NULL,
	[CURRENCY] [varchar](50) NULL,
	[Late Qty] [real] NULL,
	[Late Qty Proportion] [real] NULL,
	[Late Qty EX] [real] NULL,
	[On Time Supportability] [real] NULL,
	[On Time Supportability Proportion] [real] NULL,
	[On Time Supportability EX] [real] NULL,
	[Demand Qty] [real] NULL,
	[Demand Qty Proportion] [real] NULL,
	[Demand Qty EX] [real] NULL,
	[On Time Quantity] [real] NULL,
	[On Time Quantity Proportion] [real] NULL,
	[On Time Quantity EX] [real] NULL,
	[Un-Supportability] [real] NULL,
	[Un-Supportability Proportion] [real] NULL,
	[Un-Supportability EX] [real] NULL,
	[TOTAL_SELLIN_TOTAL] [real] NULL,
	[TOTAL_SELLIN] [real] NULL,
	[TOTAL_SELLIN_PROPORTION] [real] NULL,
	[$_SELL_IN_PER_PIECE] [real] NULL,
	[TOTAL_SELLIN_VALUE_TOTAL] [real] NULL,
	[TOTAL_SELLIN_VALUE] [real] NULL,
	[TOTAL_SELLIN_VALUE_EX] [real] NULL,
	[TOTAL_CDP_TOTAL] [real] NULL,
	[TOTAL_CDP] [real] NULL,
	[TOTAL_CDP_PROPORTION] [real] NULL,
	[TOTAL_CDP_VALUATION] [real] NULL,
	[TOTAL_SELLTHRU_TOTAL] [real] NULL,
	[TOTAL_SELLTHRU] [real] NULL,
	[TOTAL_SELLTHRU_PROPORTION] [real] NULL,
	[$_SELL_THRU_PER_PIECE] [real] NULL,
	[$_SELL_THRU_PER_PIECE_EX] [real] NULL,
	[TOTAL_SELLTHRU_VALUE_TOTAL] [real] NULL,
	[TOTAL_SELLTHRU_VALUE_EX] [real] NULL,
	[FINAL_PRICE_USD] [varchar](50) NULL,
	[FINAL_PRICE_LOCAL] [varchar](50) NULL,
	[EXCHANGE RATE FINAL PRICE] [varchar](50) NULL,
	[EXCHANGE RATE DB] [varchar](50) NULL,
	[LOAD_MONTH] [varchar](50) NULL,
	[LOAD_DATE] [varchar](50) NULL,
	[CYCLE] [date] NULL,
	[RUN_TYPE] [varchar](15) NULL,
	[archLoadDate] [datetime] NULL
) ON [PRIMARY]

GO
ALTER TABLE [SUPPLY_COMMIT].[DEMAND_SUPPORTABILITY_Temp] ADD  CONSTRAINT [DF_DEMAND_SUPPORTABILITY_Temp_archLoadDate]  DEFAULT (getdate()) FOR [archLoadDate]
GO


SET ANSI_PADDING OFF
GO
