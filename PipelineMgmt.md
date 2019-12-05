# Best practices for Pipeline Executing with Azure databricks

Azure data factory can orchestrate azure Databricks notebook. What would be some best practices to organize notebooks jobs.

## Use Case

In most Data engineering and ETL scenario when working with multiple data sources which are internal and external coming from various source systems and then converge that into a way to process the data effectveliy is a challenging concept. Say for example the data lake or warehouse or repo has like 50 objects or tables or dataset and do load all of them there is compute and time becomes a bottleneck.

The challenge here is do we run each table sequentially, or should we run all the table's in parallel manner, Or we can also group business function like supply chain, purchasing, sales, order, return, service, support, master data and create pipelines for each business function and then have each pipeline inside have each table run sequentially or can we make it parallel. What would be the best approach?

## things to consider

- Easy Job troublshooting
- Ease of finding which job failed
- Ease of maininting Code
- Ease of organzing business logic
- Azure databricks Parent and child based notebooks
- Data volume size to be processed
- Business logic dependecies for data processed
- Data quality check
- Data security processing
- Data catalog/MDM integration

## Option 1

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/imgadf1.jpg "Parallel")

## Option 2

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/imgadf2.jpg "Sequential")