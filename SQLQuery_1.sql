
select * from etlbatch;

insert into etlbatch(batchname,startdatetime,batchstatus) values('ETLADFRun1',GETDATE(),'Running');

drop procedure createbatch;

CREATE PROCEDURE createbatch
AS
update etlbatch set enddatetime = GETDATE(), batchstatus = 'Completed', totaltimesec = 0 where batchstatus = 'Running';

insert into etlbatch(batchname,startdatetime,batchstatus) values('ETLADFRun1',GETDATE(),'Running');

Select @@IDENTITY as batchid;

GO
EXEC createbatch

drop procedure closebatch;

create procedure closebatch 
    @batchid int, 
    @enddatetime datetime
AS
update etlbatch set enddatetime = GETDATE(), batchstatus = 'Completed', totaltimesec = DATEDIFF(s,startdatetime,GETDATE()) 
where id = @batchid
Go

declare @currdate as DATETIME;
Set @currdate = GETDATE();
Exec closebatch '1',@currdate;

select * from etlbatch order by id desc;