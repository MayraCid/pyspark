from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Example-3_7_1") \
        .getOrCreate()

    schema = StructType([StructField("Id", IntegerType(), False), StructField("First", StringType(), False),
                         StructField("Last", StringType(), False),
                         StructField("Url", StringType(), False),
                         StructField("Published", StringType(), False),
                         StructField("Hits", IntegerType(), False),
                         StructField("Campaigns", ArrayType(StringType(), True), False)])
    file_with_schema = spark.read.schema(schema).json("C://LearningSpark_GitHub//LearningSparkV2-master//chapter3//data//blogs.json")
    file_with_schema.show()
    file_with_schema.printSchema()
    ## imprime el json con el schema que nosotros conocemos y no con el predeterminado

    print(file_with_schema.columns) #te imprime los argumentos de las columnas
    print(file_with_schema.Id) #es el equivalente a file_with_schema.col('Id)
    file_with_schema.select(expr("Hits * 2")).show(2) #te regresa la columna con operación hecha a toda ella
    file_with_schema.withColumn("Big Hitters", (expr("Hits > 10000"))).show() #Crea una nueva columna donde los hits
    #son o no mayores o 10000. El withcolum regresa un nuevo dataframe añadiendo una nueva columna
    #o bien remplazando la existente que tiene el mismo nombre

    # Vamos a crear una nueva columna concatenando 3 columnas y lo mostraremos
    file_with_schema.withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id"))) \
        .select(col("AuthorsId")) \
        .show(4)
    #file_with_schema.select(expr("Hits")).show(2)
    #file_with_schema.select(col("Hits")).show(3)
    file_with_schema.select("Hits").show(2)

    #Ahora ordenaremos desc la columna Id y se hace con SORT
    file_with_schema.sort(col("Id").desc()).show()

