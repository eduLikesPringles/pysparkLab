# UNCOMMENT CODE A YOU GO TO CHECK FUNCTIONALITY


# SPARK DATAFRAME BASICS

# these are some basic dataframe methods

from numpy import true_divide
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Basics').getOrCreate()

df = spark.read.json('../Spark_Dataframes/people.json')
df.show()
df.printSchema()
df.columns
df.describe()
df.describe().show()

# these set of instructions are a good practice in order to perform good quality ETLs with pyspark

from pyspark.sql.types import StructField, StringType, IntegerType, StructType

data_schema = [StructField('age', IntegerType(), True), StructField('name', StringType(), True)] # define desired output data schema
final_struc = StructType(fields = data_schema) # define desired output data schema
df = spark.read.json('../Spark_Dataframes/people.json', schema = final_struc) # read the json forcing the schema into the predefined one
df.printSchema()

# some useful methods to turn dictionaries into column objects or dataframe objects

type(['age']) # returns a column object
type(df.select('age')) # returns a dataframe object
type(df.head(2)[0]) # returns a row object
df.select(['age', 'name']).show() # returns selected columns of df
df.withColumn('double_age', df['age']*2).show() # inserts a new column with the appropriate transformation
df.withColumnRenamed('age', 'my_new_age').show() # rename an already existing column without changes

# let's look at some sql!

df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people")
results.show()

new_results = spark.sql("select * from people where age = 30")
new_results.show()

# SPARK DATAFRAME BASIC OPERATIONS

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('../Spark_Dataframes/appl_stock.csv', inferSchema = True, header = True)
df.printSchema()
df.show()
df.head(3)[0]
df.filter("Close < 500").select(['Open', 'Close']).show()
df.filter(df['Close'] < 500).select('Volume').show()

# multiple filter conditions

df.filter( ( df['Close'] < 200 ) & ~( df['Open'] < 200) ).show() # filters where close is less than 200 and open is not less than 200

result = df.filter(df['Low'] == 197.16).collect() # you can collect results in a variable and change its type
row = result[0]
row.asDict()['Volume']

# GROUP BY AND AGGREGATE OPERATIONS

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('aggs').getOrCreate()

df = spark.read.csv('../Spark_Dataframes/sales_info.csv', inferSchema = True, header = True)
df.show()
df.printSchema()

df.groupBy("Company").mean().show()

df.agg({'Sales':'max'}).show() # show the max of sales

group_data = df.groupBy("Company") # another way of doing the same stuff (more loopable)
group_data.agg({'Sales':'max'}).show() # another way of doing the same stuff (more loopable)

from pyspark.sql.functions import countDistinct, avg, stddev

df.select(avg('Sales').alias('Average Sales')).show() # ways of aggregating columns by average or stddev
df.select(stddev('Sales')).show()

from pyspark.sql.functions import format_number # another way of showing stddev but to 2 sf and renaming
sales_std = df.select(stddev("Sales").alias('std'))
sales_std.select(format_number('std', 2)).show() 

df.orderBy("Sales").show() # orders ascending
df.orderBy(df['Sales'].desc()).show # orders descending

#MISSING DATA

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('miss').getOrCreate()

df = spark.read.csv('../Spark_Dataframes/ContainsNull.csv', inferSchema = True, header = True)
df.show()
df.printSchema()

df.na.drop().show()
df.na.drop(thresh = 2, how = 'all').show()
df.na.drop(subset = ['Sales']).show()

df.na.fill('FILL VALUE').show() # fills nulls with the value 'NULL VALUE'
df.na.fill(0).show() # fills 0 into numeric values only
df.na.fill('No Name',  subset = ['Name']).show() # fills null values on the Name column

from pyspark.sql.functions import mean
mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val [0][0] # this gets the numerical output of the mean (the original output is a df I think)
df.na.fill(mean_sales, ['Sales']).show()
# all this stuff can be done in just one line. pretty ugly. not readable.

# DATES AND TIMESTAMPS

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('dates').getOrCreate()
df = spark.read.csv('../Spark_Dataframes/appl_stock.csv', inferSchema = True, header = True)

df.select(['Date', 'Open']).show()

from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year, weekofyear, format_number, date_format
df.select(dayofmonth(df['Date'])).show()

df.select(year(df['Date'])).show() # this is a cool sample ETL messing with dates!
newdf = df.withColumn("Year", year(df['Date']))
result = newdf.groupBy("Year").mean().select(["Year", "avg(Close)"])
new = result.withColumnRenamed("avg(Close)", "Average Closing Price")
new.select(['Year', format_number('Average Closing Price', 2).alias("Avg Close")]).show()