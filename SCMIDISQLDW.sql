drop table sc_location_history_dim;

create table sc_location_history_dim (		
rctl_load_dttm	datetime2	,
rctl_update_dttm	datetime2	,
src_rctl_load_dttm	datetime2	,
src_rctl_update_dttm	datetime2	,
time_partition_key	int	,
source_name	varchar(100)	,
event_produced_time	datetime2	,
location_key	varchar(36)	,
valid_from_dttm	datetime2	,
valid_to_dttm	datetime2	,
valid_from_dttm_epoch	BIGINT	,
valid_to_dttm_epoch	BIGINT	,
location_number	varchar(32)	,
location_type	varchar(32)	,
location_name	varchar(100)	,
code	varchar(32)	,
alias	varchar(100)	,
brand_name	varchar(100)	,
number_drivethru_lanes	INT	,
established_indicator	varchar(32)	,
intersection	varchar(200)	,
number_terminals	INT	,
status	varchar(32)	,
address_line1	varchar(200)	,
address_line2	varchar(100)	,
city	varchar(100)	,
country	varchar(100)	,
county	varchar(100)	,
state	varchar(3)	,
zip_code	varchar(32)	,
latitude	float	,
longitude	float	,
state_name	varchar(100)	,
timezone_id	varchar(12)	,
phone_primary	varchar(32)	,
phone_altern1	varchar(32)	,
phone_altern2	varchar(32)	,
fax_primary	varchar(32)	,
fax_altern1	varchar(32)	,
fax_altern2	varchar(32)	,
email_primary	varchar(50)	,
email_altern1	varchar(50)	,
email_altern2	varchar(50)	,
web_primary	varchar(100)	,
web_altern1	varchar(100)	,
web_altern2	varchar(100)	,
mobile_primary	varchar(32)	,
mobile_altern1	varchar(32)	,
mobile_altern2	varchar(32)	,
national_provider_id	varchar(32)	,
tier	INT	,
district_code	varchar(32)	,
district_type	varchar(32)	,
area_code	varchar(32)	,
area_type	varchar(32)	,
region_code	varchar(32)	,
region_type	varchar(32)	,
operation_code	varchar(32)	,
operation_type	varchar(32)	,
return_ship_preference	varchar(32)	,
store_type_code	varchar(32)	,
store_type_description	varchar(50)	,
tax_rate	float	,
tax_calculation_type	varchar(32)	,
rolled_out_indicator	bit	
) 
WITH		
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED INDEX (location_key)	
);		

Drop table sc_supplier_catalog_history_dim;

create table sc_supplier_catalog_history_dim (		
rctl_load_dttm	datetime2	,
rctl_update_dttm	datetime2	,
source_name	varchar(100)	,
event_produced_time	datetime2	,
supplier_catalog_key	varchar(36)	,
catalog_code	varchar(36)	,
supplier_code	varchar(32)	,
supplier_key	varchar(36)	,
valid_from_dttm	datetime2	,
valid_to_dttm	datetime2	,
valid_from_dttm_EPOCH	BIGINT	,
valid_to_dttm_EPOCH	BIGINT	,
upc	varchar(32)	,
cost	float	,
cost_iso_code	varchar(10)	,
cost_price_effective_date	datetime2	,
consignment_indicator	bit	,
drop_ship_indicator	bit	,
persistence_date_time	datetime2	,
supplier_lead_time	INT	
) WITH		
(		
DISTRIBUTION = REPLICATE,
CLUSTERED INDEX (supplier_catalog_key)		
);		

Drop table cfs_team_member_hist_dim;

create table cfs_team_member_hist_dim (		
src_rctl_load_dttm	datetime2	,
src_rctl_update_dttm	datetime2	,
rctl_load_dttm	datetime2	,
rctl_update_dttm	datetime2	,
source_name	varchar(50)	,
event_timestamp	datetime2	,
team_member_key	varchar(36)	,
valid_to_dttm	datetime2	,
valid_from_dttm	datetime2	,
valid_from_dttm_epoch	BIGINT	,
valid_to_dttm_epoch	BIGINT	,
first_name	varchar(50)	,
middle_name_initial	varchar(50)	,
last_name	varchar(50)	,
initials	varchar(10)	,
suffix	varchar(50)	,
team_member_code	varchar(32)	,
team_member_type	varchar(32)	,
status	varchar(32)	,
national_provider_id	varchar(32)	,
location_number	varchar(32)	,
location_type	varchar(32)	,
sso_user_id	varchar(32)	,
email	varchar(100)	,
work_phone	varchar(32)	,
home_phone	varchar(32)	,
position_code	varchar(32)	,
idp_employee_type	varchar(32)	,
idp_user_type	varchar(32)	,
lastupdate_datetime	datetime2	,
lastupdate_user_code	varchar(32)	,
lastupdate_user_type	varchar(32)	,
lastupdate_location_code	varchar(32)	,
lastupdate_location_type	varchar(32)	
) WITH		
(		
DISTRIBUTION = REPLICATE,
CLUSTERED INDEX (team_member_key)
);		

drop table sc_supplier_history_dim;

create table sc_supplier_history_dim (		
rctl_load_dttm	datetime2	,
rctl_update_dttm	datetime2	,
source_name	varchar(100)	,
event_produced_time	datetime2	,
supplier_key	varchar(36)	,
valid_from_dttm	datetime2	,
valid_to_dttm	datetime2	,
valid_from_dttm_epoch	BIGINT	,
valid_to_dttm_epoch	BIGINT	,
supplier_code	varchar(32)	,
supplier_type	varchar(32)	,
supplier_name	varchar(100)	,
supplier_application_code	varchar(32)	,
supplier_status	Varchar(32)	,
supplier_short_name	varchar(50)	,
address	varchar(100)	,
city	varchar(50)	,
country	varchar(50)	,
state	varchar(3)	,
zipcode	varchar(32)	,
contact	varchar(50)	,
phone_number	varchar(32)	,
fax_number	varchar(32)	,
license_number	varchar(32)	,
license_type	varchar(32)	,
trading_partner_id	varchar(32)	,
item_routing_flag	varchar(50)	,
default_ordering_qualifier	varchar(50)	,
auto_release_flag	varchar(50)	,
manual_transaction	varchar(32)	,
add_substitute_item	varchar(32)	,
customer_number	varchar(32)	,
supplier_lead_time	INT	,
minimum_order_quantity	INT	,
rx_controlled_drugs	varchar(5)	,
delivery_type	varchar(32)	,
purchasing_contact	varchar(50)	,
supplier_establish_date	datetime2	,
src_rctl_load_dttm	datetime2	,
src_rctl_update_dttm	datetime2	
) WITH		
(		
DISTRIBUTION = REPLICATE,
CLUSTERED INDEX (supplier_key)
);		

drop table sc_product_history_dim;

create table sc_product_history_dim (		
rctl_load_dttm	datetime2	,
rctl_update_dttm	datetime2	,
source_name	varchar(100)	,
event_produced_time	datetime2	,
product_key	varchar(36)	,
valid_from_dttm	datetime2	,
valid_to_dttm	datetime2	,
valid_from_dttm_epoch	BIGINT	,
valid_to_dttm_epoch	BIGINT	,
product_code	varchar(32)	,
product_type	varchar(32)	,
barcode	varchar(100)	,
brand_generic_type	varchar(32)	,
category_code	varchar(32)	,
category_description	varchar(100)	,
chemical_id	varchar(32)	,
chemical_name	varchar(100)	,
crossover_indicator	bit	,
department_number	varchar(32)	,
dosage_form	varchar(50)	,
federal_drug_class	varchar(5)	,
product_description	varchar(100)	,
item_supply_source	varchar(32)	,
manufacturer_name	varchar(100)	,
manufacturer_name_abbr	varchar(50)	,
package_quantity	INT	,
package_size	float	,
package_size_unit_of_measure	Varchar(32)	,
package_type	varchar(32)	,
product_fullname	varchar(100)	,
product_name	varchar(100)	,
ndc	varchar(32)	,
status	varchar(32)	,
storage_location	varchar(50)	,
product_strength	varchar(32)	,
upc	varchar(32)	,
wic	varchar(32)	,
quick_code	varchar(32)	,
regulated_shipment_indicator	bit	,
strength_unit_of_measure	varchar(32)	,
unit_dose_package	varchar(32)	,
upc_check_digit	varchar(32)	,
specialty_indicator	bit	,
src_rctl_load_dttm	datetime2	,
src_rctl_update_dttm	datetime2	
) WITH		
(		
 DISTRIBUTION = REPLICATE,
 CLUSTERED INDEX (product_key)
);		


create table sc_stock_movement_trx_fct (		
rctl_load_dttm	datetime2	,
rctl_update_dttm	datetime2	,
source_name	varchar(32)	,
event_timestamp	datetime2	,
location_key	varchar(36)	,
location_ref_timestamp	datetime2	,
location_code	varchar(32)	,
location_type	varchar(32)	,
location_timezone_id	varchar(32)	,
product_key	varchar(32)	,
product_ref_timestamp	datetime2	,
product_code	varchar(32)	,
product_identifier_code	varchar(32)	,
product_identifier_type	varchar(32)	,
date_key	int	,
time_partition_key	int	,
stock_movement_datetime	datetime2	,
current_quantity	decimal(18,3)	,
previous_quantity	decimal(18,3)	,
delta_quantity	decimal(18,3)	,
operation_type	varchar(50)	,
stock_movement_type	varchar(32)	,
reason	varchar(32)	,
received_from_process	varchar(32)	,
service_type	varchar(32)	,
service_code	varchar(36)	,
external_service_code	varchar(100)	,
received_from_datetime	datetime2	,
src_rctl_load_dttm	datetime2	,
src_rctl_update_dttm	datetime2	,
lastupdate_timestamp	datetime2	,
lastupdate_user_code	varchar(32)	,
lastupdate_user_type	varchar(32)	,
lastupdate_location_code	varchar(32)	,
lastupdate_location_type	varchar(32)	
)		
WITH		
(		
distribution = hash(location_key),	
CLUSTERED COLUMNSTORE INDEX,
PARTITION ( time_partition_key RANGE RIGHT FOR VALUES ( 2020,2019,2018)	)	
);		

CREATE TABLE [dbo].[YellowCab_Rawdata]
(
    [vendorID] [varchar](3) NULL,
    [tpepPickupDateTime] [datetime] NULL,
    [tpepDropoffDateTime] [datetime] NULL,
    [passengerCount] [int] NULL,
    [tripDistance] [float] NULL,
    [puLocationId] [varchar](3) NULL,
    [doLocationId] [varchar](3) NULL,
    [startLon] [float] NULL,
    [startLat] [float] NULL,
    [endLon] [float] NULL,
    [endLat] [float] NULL,
    [rateCodeId] [int] NULL,
    [storeAndFwdFlag] [varchar](100) NULL,
    [paymentType] [varchar](100) NULL,
    [fareAmount] [float] NULL,
    [extra] [float] NULL,
    [mtaTax] [float] NULL,
    [improvementSurcharge] [varchar](100) NULL,
    [tipAmount] [float] NULL,
    [tollsAmount] [float] NULL,
    [totalAmount] [float] NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [dbo].[GreenCab_RawData]
(
    [vendorID] [int] NULL,
    [lpepPickupDatetime] [datetime] NULL,
    [lpepDropoffDatetime] [datetime] NULL,
    [passengerCount] [int] NULL,
    [tripDistance] [float] NULL,
    [puLocationId] [varchar](3) NULL,
    [doLocationId] [varchar](3) NULL,
    [pickupLongitude] [float] NULL,
    [pickupLatitude] [float] NULL,
    [dropoffLongitude] [float] NULL,
    [dropoffLatitude] [float] NULL,
    [rateCodeID] [int] NULL,
    [storeAndFwdFlag] [varchar](1) NULL,
    [paymentType] [int] NULL,
    [fareAmount] [float] NULL,
    [extra] [float] NULL,
    [mtaTax] [float] NULL,
    [improvementSurcharge] [varchar](4) NULL,
    [tipAmount] [float] NULL,
    [tollsAmount] [float] NULL,
    [ehailFee] [float] NULL,
    [totalAmount] [float] NULL,
    [tripType] [int] NULL,
    [puYear] [int] NULL,
    [puMonth] [int] NULL
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)
GO


select top 200 * from dbo.YellowCab_Rawdata

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23987hxJ#KL95234nl0zBe';
GO


select top 50000 * from [dbo].[T_2125710e8ebd4a1b92291508d4fa56c6]

select count(*) from [dbo].[T_2125710e8ebd4a1b92291508d4fa56c6]
