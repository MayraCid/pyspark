from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Example-3_7") \
        .getOrCreate()

    #Lectura archivo json en un DataFrame sin usar esquema
    file_DF = spark.read.json("C://LearningSpark_GitHub//LearningSparkV2-master//chapter3//data//blogs.json")
    file_DF.show()
    file_DF.printSchema()
    #print(file_DF.schema) #nos imprime el esquema que puedo utilizar
    print(file_DF.Id) #es el equivalente a file_with_schema.col('Id)
    print(file_DF.columns)#te imprime los argumentos de las columnas
    file_DF.select(expr("Hits * 2")).show(2) #te regresa la columna con operación hecha a toda ella
    file_DF.withColumn("Big Hitters", (expr("Hits > 10000"))).show() #Crea una nueva columna donde los hits
    #son o no mayores o 10000. El withcolum regresa un nuevo dataframe añadiendo una nueva columna
    #o bien remplazando la existente que tiene el mismo nombre

    #Vamos a crear una nueva columna concatenando 3 columnas y lo mostraremos
    file_DF.withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))\
        .select(col("AuthorsId"))\
        .show()

    #Lo siguiente realiza los mismo pero expresado en diferentes formas mostando que expr es lo mismo que el método col
    #file_DF.select(expr("Hits")).show(2)
    #file_DF.select(col("Hits")).show(2)
    #file_DF.select("Hits").show(2)

    # Ahora ordenaremos desc la columna Id y se hace con SORT
    file_DF.sort(col("Id").desc()).show()


