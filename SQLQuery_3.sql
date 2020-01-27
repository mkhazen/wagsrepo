
select top 100 * from dbo.YellowCab_Rawdata;

select count(*) from YellowCab_Rawdata;

Select 
DATEPART(YEAR,tpepPickupDateTime),
DATEPART(MONTH,tpepPickupDateTime),
vendorID,
count(*) 
from 
dbo.YellowCab_Rawdata 
group by DATEPART(YEAR,tpepPickupDateTime),DATEPART(MONTH,tpepPickupDateTime), vendorID 