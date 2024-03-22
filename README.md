# End-to-End-Maritime-Data-Engineering-Project
Here, we have created a pipeline which extracts, transforms and loads maritime data which is collected by AIS organization on daily basis.

# Problem Statement
AIS is a global organisation which fetches marine data from all over the globe ranging from private to commercial vessels on daily basis. This data is extracted through a GPS system using satellite netwok.
Now as the data is retrived on daily basis, We need to process the data on daily basis and send the consumable data to the Data Analytics team. 

# Approach
In our case we have stored the intial data in AWS S3, from which we transfer the data to Azure Blob storage i.e. Azure Data Lake using required Azure key vaults, from there the Azure Databricks fetches the data using mount files using DBFS. Here the data is read, transformed based on plausible industry scenarios and written down again to the Azure blob storage. 
