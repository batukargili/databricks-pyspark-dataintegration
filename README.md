# Data Integration PySpark

Transaction and Article csv files cleaned, validated and combined to create target table using PySpark.

## Requirements

- Databricks account

## Deployment
 - Create Cluster on Databricks
 - Upload Transaction and Article csv files to DBFS
 - Import one of the DBC, iipynb or .py files under notebooks folder.


## Integration Flow
1. **Import Libraries and Read CSV Data** read_csv_dbfs function reads given csv file an loads into PySpark Dataframes. describe_df function gives an overview of the dataframe.
   
2. **Cleaning and Validating Transaction/Article Data** formats bad data and clean columns,
   
3. **Join Dataframes** Joined two dataframes on Article_id column. As Article_id defined as Foreign Key, records that have matching values in both dataframes joined using inner join method. 
   
4. **Sort By date and time** Sort by date, than transaction time.
   
5. **Check Duplicates** duplicate_controller function drops duplicate rows if exists
   
6. **Add Index Column and Write DF to Target Table**  






