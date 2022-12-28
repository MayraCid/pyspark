from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Example-3_7") \
        .getOrCreate()

    #Lectura archivo json en un DataFrame sin usar esquema
    file_DF = spark.read.json("C://LearningSpark_GitHub//LearningSparkV2-master//chapter3//data//blogs.json")
    file_DF.show()
    file_DF.printSchema()


