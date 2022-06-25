from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, FloatType
import os
from pyspark.sql import SparkSession

# make an instance of a SparkSession called 'spark'
spark = SparkSession.builder.master('local').getOrCreate()

custom_schema = StructType([
    StructField('index', IntegerType()),
    StructField('artist_popularity', LongType()),
    StructField('followers', LongType()),
    StructField('genres', StringType()),
    StructField('id', StringType()),
    StructField('name', StringType()),
    StructField('track_id', StringType()),
    StructField('track_name_prev', StringType()),
    StructField('type', StringType())])

"""Extract Information"""
spark_df = (spark.read.format("csv"
                              ).options(header="true"
                                        ).schema(custom_schema
                                                 ).load("data/spotify_artists.csv"))


"""Profile the Data:"""

# Show a description (summary) of the Spark DataFrame.
spark_df.describe
# Print the schema of the DataFrame.
spark_df.printSchema()
# Select and show just the first 10 values in the 'name' and 'genres' columns.
spark_df.select('name', 'genres').show(8)
# Where the genre is an empty list, replace it with ['elevator music'].
spark_df = spark_df.withColumn('genres', regexp_replace(
                                'genres', r"\[\]", "['elevator music']"))
spark_df.select('name', 'genres').show(8)
# For the columns 'artist_popularity' and 'followers', cast the data type as integers.
# done in the schema, but ill throw in the code, so you know i know it
spark_df = spark_df.withColumn('artist_popularity', col(
                    'artist_popularity').cast(IntegerType()))
spark_df = spark_df.withColumn('followers', col(
                    'followers').cast(IntegerType()))
# Sort the data in descending order by number of followers.
spark_df.select('*').sort('followers', ascending=False).show(5)

"""process to change the artist popularity to percent and rename the column"""

# user defined function to divide x by 100
pop_contest = udf(lambda x: x/100)
# apply our udf to the column artist popularity
pop_contest_df = spark_df.withColumn('artist_popularity',
                                     pop_contest(spark_df['artist_popularity']))
# Rename the column 'popularity_percent'.
pop_contest_df = spark_df.withColumnRenamed(
    'artist_popularity', 'popularity_percent')
# print the least cool kids to chow our function is working
pop_contest_df.select('popularity_percent', 'followers',
                      'name', 'genres').sort('popularity_percent').show(8)

"""Extract Information"""

# Group the data by artist popularity, and show the count for each group.
spark_df = spark_df.withColumn('artist_popularity', spark_df[
    'artist_popularity'].cast('double'))
spark_df.printSchema()

spark_df.groupBy('artist_popularity').sum('artist_popularity').show(8)

"""save dataframe as a parquet"""

# write the code to save the DataFrame as a Parquet file in the /data directory.
spark_df.write.parquet('data/spotify.parquet')

