from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("First") \
        .master("local[*]") \
        .getOrCreate()
    #Sólo te dice donde esta guardado
    input_file_path = 'C://Users//mayra.cid//Documents//Rutas//tech.txt'

    tech_rdd = spark.sparkContext.textFile(input_file_path)

    print("printing")

    print(tech_rdd.collect())

    print("Aplication Completed")

#Lo que hizo fue traer a pantalla
#todo lo que esta en el archivo indicado.