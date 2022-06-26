from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import LongType, StringType, StructField, StructType,IntegerType
from pyspark.sql import SparkSession

# make an instance of a SparkSession called 'spark'
spark = SparkSession.builder.master('local').getOrCreate()

# create a custom schema to ensure data types and column names
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
# create a spark dataframa from a csv with our custom schema
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
spark_df.select('name', 'genres').show(10)
# Where the genre is an empty list, replace it with ['elevator music'].
spark_df = spark_df.withColumn('genres', regexp_replace(
                                'genres', r"\[\]", "['elevator music']"))
spark_df.select('name', 'genres').show(10)
# For the columns 'artist_popularity' and 'followers', cast the data type as integers.
# done in the schema, but ill throw in the code, so you know i know it
spark_df = spark_df.withColumn('artist_popularity', col(
                    'artist_popularity').cast(IntegerType()))
spark_df = spark_df.withColumn('followers', col(
                    'followers').cast(IntegerType()))
# Sort the data in descending order by number of followers.
spark_df.select('*').sort('followers', ascending=False).show(10)


"""process to change the artist popularity to percent and rename the column"""
# user defined function to divide x by 100
pop_contest = udf(lambda x: x/100)
# apply our udf to the column artist popularity
pop_contest_df = spark_df.withColumn('artist_popularity', 
                pop_contest(spark_df['artist_popularity']))
# Rename the column 'popularity_percent'.
pop_contest_df = spark_df.withColumnRenamed('artist_popularity', 
                                            'popularity_percent')
# change datatype of the percent column from a string to a float
pop_contest_df = pop_contest_df.withColumn('popularity_percent', 
                            pop_contest_df['popularity_percent'
                                    ].cast('float'))

"""Extract Information"""
# Show only the values in the DataFrame that have 'Queen' in the name.
spark_df.select('*').filter(spark_df.name.contains('Queen')).show(8)
# Group the data by artist popularity, and show the count for each group.
spark_df.groupBy('artist_popularity').sum('artist_popularity').show(10)

"""save dataframe as a parquet"""
# write the code to save the DataFrame as a Parquet file in the /data directory.
spark_df.write.parquet('data/spotify.parquet')

