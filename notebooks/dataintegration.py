# Databricks notebook source
# MAGIC %md
# MAGIC ###  Data Integration Project
# MAGIC 
# MAGIC Transaction and Article csv files cleaned, validated and combined to create target table using PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries & Read Csv Files

# COMMAND ----------

import pyspark
from pyspark.sql.functions import col, isnan, count, when, regexp_replace, to_date, monotonically_increasing_id
from pyspark.sql.types import IntegerType, FloatType

# COMMAND ----------

def read_csv_dbfs(file_name, infer_schema, first_row_is_header, delimiter):
    """
    Reads csv file from DBFS's /FileStore/tables/ location into PySpark DataFrame.
    return: df 
    """
    file_location = "/FileStore/tables/" + file_name
    file_type = "csv"
    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(file_location)
    
    return df

# COMMAND ----------

def describe_df(df):
    """
    Shows a dataframe's schema, shape and distinct-duplicate row counts
    """
    # Schema
    df.printSchema()
    # Shape
    print(f"Row Count: {df.count()}, Column Count: {len(df.columns)}")
    # Distinct & Duplicate row count
    distinct_row_count = df.distinct().count()
    duplicate_row_count = df.count() - df.distinct().count()
    print(f"Distinct Row Count: {distinct_row_count}, Duplicate Row Count: {duplicate_row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read and Describe TRANSACTION data 

# COMMAND ----------

# Read TRANSACTION.csv into dataframe
transaction_df = read_csv_dbfs(file_name="TRANSACTION.csv", infer_schema="true", first_row_is_header="true", delimiter=",")

# COMMAND ----------

# Describe Transaction DF
describe_df(transaction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read and Describe ARTICLE data 

# COMMAND ----------

# Read TRANSACTION.csv into dataframe
article_df = read_csv_dbfs(file_name="ARTICLE.csv", infer_schema="true", first_row_is_header="true", delimiter=",")

# COMMAND ----------

# Describe ARTICLE DF
describe_df(article_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Transaction Data

# COMMAND ----------

# MAGIC %md
# MAGIC - Unwanted float values and int values like "31.99-"" and "1.000-" detected at "SALES_PRICE_AT_CASH_DESK0RPA_SAT","SALES_PRICE_PLANNED/SOL/LOC0086C","VAT0RPA_TAM" and "ARTICLE_COUNT0RPA_RLQ" columns. This values transformed and casted to desired format.

# COMMAND ----------

transaction_float_columns=["SALES_PRICE_AT_CASH_DESK0RPA_SAT","SALES_PRICE_PLANNED/SOL/LOC0086C","VAT0RPA_TAM"]
transaction_int_columns = ["ARTICLE_COUNT0RPA_RLQ"]

# COMMAND ----------

# All numeric values can be casted to int so cast("int") used,  
def transform_numeric_columns(df, column_list):
    """
    Dirty rows like  "31.99-" validated to a float like string.
    return: df
    """
    for column in transaction_numeric_columns:
        df = df.withColumn(column, when(col(column).cast("int").isNull(),regexp_replace(column, '-', '')).otherwise(col(column)))
    
    return df
    

# COMMAND ----------

def cast_numeric_columns(df, column_list, data_type):
    """
    Cast Columns to desired data types if all rows are validated.
    return: df
    """
    for column in column_list:
        unmatched_row_count = df.filter(col(column).cast(data_type).isNull()).count()
        print(f"Column Name: {column}, Not{data_type} row Count: {unmatched_row_count}")
        if unmatched_row_count == 0:
            print(f"All values can be casted to {data_type} data type.")
            df = df.withColumn(column,col(column).cast(data_type))
        else:
            print("Ups ! Some values still include dirty characters like: ")
            print(df.filter(col(column).cast(data_type).isNull()).show())
    return df

# COMMAND ----------

def duplicate_controller(df):
    """
    Drops duplicate rows if exists
    return: df
    """
    distinct_row_count = df.distinct().count()
    duplicate_row_count = df.count() - distinct_row_count
    if duplicate_row_count > 0:
        df = df.dropDuplicates(targettable_df.columns)
        print("Droped duplicate rows")
    else :
        print("No duplicates")
    return df

# COMMAND ----------

# Validate Transaction Data and fix rows
transaction_numeric_columns = transaction_int_columns + transaction_float_columns
transaction_df = transform_numeric_columns(transaction_df, transaction_numeric_columns)
# Cast columns to desired data types
transaction_df = cast_numeric_columns(transaction_df, transaction_float_columns, "float")
transaction_df = cast_numeric_columns(transaction_df, transaction_int_columns, "int")

# COMMAND ----------

#check dtypes
transaction_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Article Dataset 

# COMMAND ----------

# Barcode Ean value should be bigint or long but null values breaks the type so null values replaced and column casted to long
article_long_columns = ["EAN0EANUPC"]

for column in article_long_columns:
    article_df = article_df.withColumn(column, when(col(column).cast("bigint").isNull(),regexp_replace(column, 'null', '0')).otherwise(col(column)))
    
article_df = cast_numeric_columns(article_df, article_long_columns, "long")

# COMMAND ----------

article_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join Transaction and Article Dataframes by Article_Id

# COMMAND ----------

#Column names simplified for better understanding
transaction_df=transaction_df.withColumnRenamed("ARTICLE_ID0MATERIAL","FKARTICLE_ID") \
.withColumnRenamed("TRANSACTION_ID/SOL/BONKEY","TRANSACTION_ID")

# COMMAND ----------

# Joined two dataframes on Article_id column. As Article_id defined as Foreign Key, records that have matching values in both dataframes joined using inner join method. 
targettable_df = transaction_df.join(article_df, transaction_df.FKARTICLE_ID == article_df.ARTICLE_ID0MATERIAL, how= "inner").drop("ARTICLE_ID0MATERIAL")

# COMMAND ----------

# Describe Transaction DF
describe_df(targettable_df)

# COMMAND ----------

# Cast Integer type TRANSACTION_DATE column to date type
targettable_df = targettable_df.withColumn("TRANSACTION_DATE0CALDAY",col("TRANSACTION_DATE0CALDAY").cast("string")).withColumn("TRANSACTION_DATE",to_date(col("TRANSACTION_DATE0CALDAY"),"yyyyMMdd").cast("date")).drop("TRANSACTION_DATE0CALDAY")

# COMMAND ----------

# Sort by date, than transaction time -> date aggregation ?
targettable_df = targettable_df.sort(col("TRANSACTION_DATE").asc(),col("TRANSACTION_TIME0RPA_ETS2").asc())

# COMMAND ----------

# Check if duplicate rows exists and drops them if exists
targettable_df = duplicate_controller(targettable_df)

# COMMAND ----------

# As some TransactionId values occured multiple times with different Articles a new primary index key added to dataframe.  
targettable_df = targettable_df.withColumn("INDEX",monotonically_increasing_id())

# COMMAND ----------

# Final dataframe overview
display(targettable_df)

# COMMAND ----------

# Write final dataframe to target table
targettable_df.write.saveAsTable("TransactionArticle")
