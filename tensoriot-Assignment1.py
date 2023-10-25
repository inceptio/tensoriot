#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, min, max, length, from_unixtime, date_format



# create spark context
spark = SparkSession.builder.appName("tensoriot-Assignment1").getOrCreate()

#Assignment1


#parse and load only required cols from json data
schema = StructType([
    StructField('asin', StringType(), True),
    StructField('overall', DoubleType(), True), 
    StructField('reviewText', StringType(), True),
    StructField('reviewerID', StringType(), True),
    StructField('unixReviewTime', LongType(), True)
])



#input path can be input params to job 
#reviews.json is just chunk of data 
file_path = "/Users/mac/tensoriot/reviews.json"
reviews_df = spark.read.schema(schema).json(file_path)

#dropping rows which having null values ie data cleansing
reviews_no_null_df = reviews_df.na.drop()



# Get Items having the least and most rating.
total_reviews_df = reviews_no_null_df.groupBy("asin").agg(count("reviewerID").alias("total_reviews"))
min_reviews = total_reviews_df.select(min("total_reviews")).first()[0]
max_reviews = total_reviews_df.select(max("total_reviews")).first()[0]

items_with_least_reviews_df = total_reviews_df.filter(total_reviews_df["total_reviews"] ==min_reviews )
items_with_most_reviews_df = total_reviews_df.filter(total_reviews_df["total_reviews"] ==max_reviews )
items_with_most_reviews_df.show()

#Get Item having the longest reviews.

review_length_df = reviews_no_null_df.withColumn("review_length", length(reviews_no_null_df["reviewText"]))
max_review_length = review_length_df.select(max("review_length")).first()[0]
items_with_max_review_length_df = review_length_df.filter(review_length_df["review_length"] == max_review_length )
items_with_max_review_length_df.show()

# transform date to MM-dd-yyyy format.
formatted_date_df = reviews_no_null_df.withColumn("formatted_date", date_format(from_unixtime("unixReviewTime"), "MM-dd-yyyy"))
formatted_date_df.show()


#output path can be input params to job
parquet_output_path = "/Users/mac/tensoriot/output/output.parquet"

# Save the DataFrame in Parquet format
formatted_date_df.write.mode("overwrite").parquet(parquet_output_path)





