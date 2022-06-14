select min(A.archLoadDate) as archLoadDate from
(select Min(archLoadDate) as archLoadDate from MASTER_DATA.FED_LPR
union
select Min(archLoadDate) as archLoadDate from MASTER_DATA.LMRPC
union
select Min(archLoadDate) as archLoadDate from MASTER_DATA.MRP_CONTROLLER
union
select Min(archLoadDate) as archLoadDate from MASTER_DATA.MVKE_PRAT1
union
select Min(archLoadDate) as archLoadDate from MASTER_DATA.SETS
) A