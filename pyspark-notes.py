"""
	A set notes to effectively use pyspark
"""




#useful imports
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit, when, sum, lag, unix_timestamp
from pyspark.sql.window import Window



#############################
# Dataframe Manipulation
#############################

# Convert pyspark dataframe to pandas
dfpivot.toPandas()

# where
df = df.where(col('colname') >= 0)

# concat
df_od = df_od.withColumn('colname_new', concat(col('col1'), lit('-'), col('col2')))

#f ilter in
dfSample = df.filter(col('date').isin(['2017-03-13','2018-01-05']))


# groupby agg
from pyspark.sql import functions as F
dfagg = dfSample.groupby('col1').agg(\
                                     F.countDistinct('col2').alias('name2'), \
                                     F.max('col3').alias('name3'), \
                                     F.max('col4').alias('name4'))

# where Not null
df.where(col("col1").isNotNull())

# Where Null
df.where(col("col1").isNull())





#############################
# Joins
#############################
left_join = ta.join(tb, ta.name == tb.name,how='left')





#############################
# Spark SQL
#############################

# register a table
df.registerTempTable('df_name')

# run sql & assign
df = spark.sql("create table schema.table_name as select * from table_name where col > 10")




#############################
# Pyspark to Python
#############################
# Convert 2 columns to dict
keypair_rdd = df.select('col1','col2').rdd.map(lambda x : (x[1],x[0]))
dict = keypair_rdd.collectAsMap()
print(dict)

#Convert column to list
mylist = df.select('col1').rdd.flatMap(lambda x: x).collect()
print(mylist)