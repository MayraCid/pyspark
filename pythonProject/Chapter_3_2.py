# In Python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create a DataFrame using SparkSession
spark = (SparkSession \
 .builder \
 .appName("AuthorsAges") \
 .getOrCreate())

# Create a DataFrame
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35),
 ("Brooke", 25)], ["name", "age"])
data_df.show()

# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()
