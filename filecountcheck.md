# Azure Data Factory - ETL (Extract, Transform, Load) / ELT (Extract, Load, Transform)

## Count files to process batch loading

Created a incoming folder and uploaded 2 files.
Process:

1) Read the folder with all files. Only fully uploaded files are available to read Use Get Metadata.
2) Create a new array variable called filelist
3) Create a new variable string called fillistcount
4) For each file name and add it to array variable.
5) For every file take the file name and add it to filelist
6) Once the For each is completed set a variable filelistcount and assign filelist’s length variable
7) Add a If logic and the expression would be @if(equals(variables('filelistcount'),'2'),'yes','no'). If the array has 2 files then it is true or else false.
8) If true then proceed to downstream process.
9) The above logic can be until process. Until process ends when filelistcount equals to 2


## End to End Pipeline
