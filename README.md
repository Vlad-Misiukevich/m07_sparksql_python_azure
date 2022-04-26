# Installation
```bash
git clone https://github.com/Vlad-Misiukevich/m07_sparksql_python_azure.git
```
# Requirements
* Python 3.8
* Windows OS
* azure-cli
* terraform
# Usage
1. Login to Azure  
`az login`
2. Deploy infrastructure with terraform  
`terraform init`  
`terraform plan -out terraform.plan`  
`terraform apply terraform.plan`

# Description  
1. Copy data to Azure ADLS gen2 storage.
![img.png](images/img.png)  
2. Create delta tables based on data in storage account.  
![img_1.png](images/img_1.png)  
![img_2.png](images/img_2.png)  
3. Using Spark SQL calculate and visualize tasks:  
* Top 10 hotels with max absolute temperature difference by month  
![img_3.png](images/img_3.png)  
![img_4.png](images/img_4.png)  
* Weather trend for visits with extended stay  
![img_5.png](images/img_5.png)  
![img_6.png](images/img_6.png)  
4. Deploy Databricks Notebook on cluster 
![img_7.png](images/img_7.png)
![img_8.png](images/img_8.png)
![img_11.png](images/img_11.png)
5. Store data in storage container.  
![img_9.png](images/img_9.png)
![img_10.png](images/img_10.png)