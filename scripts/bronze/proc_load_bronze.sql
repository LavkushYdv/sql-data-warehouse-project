/*
The Bronze Layer is the raw data ingestion stage in a data warehouse. It loads unprocessed data from
CRM and ERP CSV files into staging tables using BULK INSERT. Each table is truncated before loading, 
and load durations are logged. 
The procedure includes error handling and prints progress for transparency.
*/

--exec bronze.load_bronze;
Create or alter procedure bronze.load_bronze as
begin
DECLARE @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
set @batch_start_time =GETDATE()
BEGIN TRY
print '=================================================';
print 'Loading Bronze Layer....';
print '=================================================';

Print '-------------------------------------------------';
Print 'Loding CRM Table';

Print '-------------------------------------------------';

--select 1/0;
--DECLARE @start_time datetime,@end_time datetime;

set @start_time =GETDATE()

Print '-------------------------------------------------';
Print 'Truncating CRM Table: crm_cust_info';

Print '-------------------------------------------------';
truncate table bronze.crm_cust_info

Print '-------------------------------------------------';
Print 'Inserting data into  CRM Table : crm_cust_info';

Print '-------------------------------------------------';


Bulk insert bronze.crm_cust_info
from 'C:\DataAnalysisProject\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
Firstrow =2,
fieldterminator=',',
Tablock
)

set @end_time =GETDATE()
print '>> Load duration: ' + cast(datediff ( second , @start_time, @end_time) as nvarchar)  + ' seconds';

--select * from bronze.crm_cust_info;
set @start_time =GETDATE()

Print '-------------------------------------------------';
Print 'Truncating CRM Table: crm_prod_info';

Print '-------------------------------------------------';
truncate table bronze.crm_prod_info
Print '-------------------------------------------------';
Print 'Inserting data into  CRM Table : crm_prod_info';

Print '-------------------------------------------------';
Bulk insert bronze.crm_prod_info
from 'C:\DataAnalysisProject\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with(
Firstrow =2,
fieldterminator=',',
Tablock
);
set @end_time =GETDATE()
print '>> Load duration: ' + cast(datediff ( second , @start_time, @end_time) as nvarchar)  + ' seconds';

--select * from bronze.crm_prd_info;
set @start_time =GETDATE()
Print '-------------------------------------------------';
Print 'Truncating CRM Table: crm_sales_details';

Print '-------------------------------------------------';
truncate table bronze.crm_sales_details
Print '-------------------------------------------------';
Print 'Inserting data into  CRM Table : crm_sales_details';

Print '-------------------------------------------------';
Bulk insert bronze.crm_sales_details
from 'C:\DataAnalysisProject\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with(
Firstrow =2,
fieldterminator=',',
Tablock
);
set @end_time =GETDATE()
print '>> Load duration: ' + cast(datediff ( second , @start_time, @end_time) as nvarchar)  + ' seconds';

Print '-------------------------------------------------';
Print 'Loding ERP Table';

Print '-------------------------------------------------';

set @start_time =GETDATE()
Print '-------------------------------------------------';
Print 'Truncating ERP Table: erp_CUST_AZ12';

Print '-------------------------------------------------';

truncate table bronze.erp_CUST_AZ12
Print '-------------------------------------------------';
Print 'Inserting data into  ERP Table : erp_CUST_AZ12';

Print '-------------------------------------------------';
Bulk insert bronze.erp_CUST_AZ12
from 'C:\DataAnalysisProject\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with(
Firstrow =2,
fieldterminator=',',
Tablock
);
set @end_time =GETDATE()
print '>> Load duration: ' + cast(datediff ( second , @start_time, @end_time) as nvarchar)  + ' seconds';

set @start_time =GETDATE()

Print '-------------------------------------------------';

Print 'Truncating ERP Table: erp_LOC_A101';

Print '-------------------------------------------------';
truncate table bronze.erp_LOC_A101
Print '-------------------------------------------------';
Print 'Inserting data into  ERP Table : erp_LOC_A101';

Print '-------------------------------------------------';
Bulk insert bronze.erp_LOC_A101
from 'C:\DataAnalysisProject\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with(
Firstrow =2,
fieldterminator=',',
Tablock
);
set @end_time =GETDATE()
print '>> Load duration: ' + cast(datediff ( second , @start_time, @end_time) as nvarchar)  + ' seconds';

set @start_time =GETDATE()
Print '-------------------------------------------------';
Print 'Truncating ERP Table: erp_PX_CAT_G1V2';

Print '-------------------------------------------------';
truncate table bronze.erp_PX_CAT_G1V2
Print '-------------------------------------------------';
Print 'Inserting data into  ERP Table : erp_PX_CAT_G1V2';

Print '-------------------------------------------------';
Bulk insert bronze.erp_PX_CAT_G1V2
from 'C:\DataAnalysisProject\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with(
Firstrow =2,
fieldterminator=',',
Tablock
)
set @end_time =GETDATE()
print '>> Load duration: ' + cast(datediff ( second , @start_time, @end_time) as nvarchar)  + ' seconds';

set @batch_end_time =GETDATE()
print 'Total load time for Bronze layer '
print '- Total Load duration: ' + cast(datediff ( second , @batch_start_time, @batch_end_time) as nvarchar)  + ' seconds';

END TRY
BEGIN CATCH
PRINT '---------------------------------------------'
PRINT 'ERROR MESSAGE: Error Occured During loading broze layer'
PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER()AS NVARCHAR);
PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE()AS NVARCHAR);
PRINT '---------------------------------------------'
END CATCH
end;

