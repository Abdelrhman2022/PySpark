# -*- coding: utf-8 -*-
"""
Created on Sat May 11 12:11:09 2024

@author: Abdelrahman Ragab
"""


# Importing necessary libraries
import pyspark
from pyspark.sql import SparkSession

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# Getting the SparkContext from the SparkSession
sc = spark.sparkContext

# Reading text file into an RDD
rdd0 = sc.textFile('sample.txt')

# Collecting and printing the contents of the RDD
rdd0.collect()

# Converting each line to uppercase and splitting by whitespace
rdd1 = rdd0.flatMap(lambda l : l.upper().split())

# Mapping each word to (word, 1)
rdd2 = rdd1.map(lambda x: (x,1))

# Taking first two elements from RDD for demonstration
rdd2.take(2)

# Reducing by key to get count of each word
rdd3 = rdd2.reduceByKey(lambda x,y : x+y)

# Collecting and printing the final word count
rdd3.collect()



#################################################################


# Adding DataFrame Solution 

# Create DataFrame from rdd2
df = spark.createDataFrame(rdd2.collect(), ['word','freq'])

# PRINT SCHEMA 

print(df.printSchema())

# AGG Sum of each words and return only first 10
df.groupBy('word').sum('freq').head(10)


