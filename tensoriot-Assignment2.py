#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession


# create spark context with required packages

spark = SparkSession.builder.appName("tensoriot-Assignment2") .config("spark.driver.extraClassPath", "/Users/mac/.m2/repository/mysql/mysql-connector-java/8.0.22/mysql-connector-java-8.0.22.jar") .getOrCreate()


#read parquet data
parquet_output_path = "/Users/mac/tensoriot/output/output.parquet"
parquet_df = spark.read.parquet(parquet_output_path)




#we can mive all this config params to constants 
jdbc_url = "jdbc:mysql://localhost:3306/tensoriot"
properties = {
    "user": "root",
    "password": "aNANt@1129"
}
table_name = "reviews"



#save dataframe to mysql
parquet_df.write.mode("overwrite").jdbc(jdbc_url, table_name, properties=properties)




