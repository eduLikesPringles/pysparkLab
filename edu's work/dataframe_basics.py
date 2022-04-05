# these are some basic dataframe methods

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