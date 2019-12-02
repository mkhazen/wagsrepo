# Azure Data Factory - ETL (Extract, Transform, Load) / ELT (Extract, Load, Transform)

## Use Case

Mostly in enterprise ETL scenario when loading from different sources. Each source can come at any time and they migh tbe from different source systems which is owned by the enterprise and also external source. So there might be scneario where there is no control on when the file can land. But would like to count the number of files to know if all files are here to be processed. The reason is only when all required files landed the ETL logic will work.

There are multiple way to do and here is one of my approach. I am counting the number of file and if required count matches the file count then good do go. We can also loop file name and check for names and then add that to the array and then validate if array as the correct names as well. For now i am going go with file counting match.

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

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img2.jpg "End to End Flow")

## Steps to count files and only if the count = 2 then process the pipeline

## Setup variables needed

Creating variables
![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img1.jpg "Create Variables")

## Setup GetMetadata

Drag and drop getmetadata and configure to point to the folder where files are going be dropped. In my case
i used blob storage and named the directory as startjob.

GetMetadata:
![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img21.jpg "Create GetMetadata")

Blob Connection configuration for getmetadata:
![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img22.jpg "Blob configuration")

## Setup Foreach

Foreach is used to loop each file in the directory and then add that to array for further processing. Once the filename is in array we can use that for further processing multiple times if we have to.

Drag and drop foreach to canvas. 

- Setup foreach to take getmetadata as input and then process the output list of folder's file names.

Go to Settings tab in properties pane in foreach.

- Select Sequential check box.
- in the items text box type in: @activity('ZipMetadata').output.childItems

> @activity('ZipMetadata').output.childItems - will give us the list of all filenames as list.


Double click foreach and will take you inside the foreach. Here we need to create 2 steps
![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img3.jpg "For Each")

- Set Variable
- Append Variable

Drag and drop Set variable to canvas. In the properties pane select variables. In the Name drop down select dummyvar and in the value type in @item().name. 

> @item().name will give us the file name with extension.

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img4.jpg "For Each")

Drag and drop Append variables to canvas. in the properties pane select Variables. In the Name drop down select filelist (array) and in the value field: @variables('dummyvar')

> @variables('dummyvar') will append the file name to the filelist array.

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img5.jpg "For Each")

## Find the length of Array to know how many files are there?

Go back to main canvas ( out of for each). Drag and drop Set variable in to the canvas and connect to foreach.

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img6.jpg "For Each")

In the properties pane go to variables. 

- Select the name as filelistcount from drop down list
- in the Value text box type in: @string(length(variables('filelist')))

> @string(length(variables('filelist'))) the above code will get the length of the filelist array which has all the filenames in the folder.

## Time to check for file count matches our requirement count

Here we are going to check if the filelist array count is equal to required file count and if true then proceed with downstream processing other wise do nothing.

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img7.jpg "IF condition")

Drag and drop if statement. Go to properties pane.

- Go to Settings tab.
- in the Expression text: @equals(variables('filelistcount'),'2')
- if true activity do the downstream processing. I am just doing a copy activity but the choice is yours.

![alt text](https://github.com/balakreshnan/wagsrepo/blob/master/images/img8.jpg "IF expression")

End of file count processing and validation.
