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
- Parameters/variables used for notebooks and passing that from ADF


## Option 1

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/imgadf1.jpg "Parallel")

## Option 2

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/imgadf2.jpg "Sequential")

## Execution logging results

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/imgadf3.jpg "Monitor")

## Option 3

Running ADB notebooks in paralel. 10 notebooks took 12 minutes. 3 notebooks took 5 minutes. 2 notebooks took 4 minutes.

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/adfadb1.jpg "Parallel")

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/adfadb2.jpg "Parallel")

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/adfadb3.png "Parallel")

Notebook for the above adf activity is here
https://github.com/balakreshnan/wagsrepo/blob/master/etlADFtest.md

## Results from different run

Azure databricks VM size DSv4_s2 - 28GB Ram and 8 Cores as each VM.

10 nodes - no scaling - 11 minutes

5 nodes - no scaling - 9 minutes

10 nodes - auto scaling starting with 6 to 10 - 10 minutes

10 nodes - auto scaling 4 to 8 - 10 minutes
