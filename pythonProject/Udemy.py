#Vamos hacer primero una SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col

if __name__ == "__main__":
    print('Iniciando lectura ...')

    spark = SparkSession \
        .builder \
        .appName("UdemyDF") \
        .master("local[*]") \
        .getOrCreate()

    #Lectura del archivo csv en un DataFrame
    Udemy_DF = spark.read.csv("C://Users//mayra.cid//Documents//Udemy_courses_Dataset//Course_info.csv")

    #Schema del CSV
    Udemy_DF.printSchema()
    print(Udemy_DF.schema)


    #Tres renglones del DataFrame
    #Udemy_DF.show(3)

    #Número de renglones
    #print("El número de filas es:", Udemy_DF.count())

    #Se ordena de forma descendente el id
    #Udemy_DF.sort(col("_c0").desc()).show(10)

    #Solo muestre los resultados donde lenguage= English y me los ordene por la
    #la columna _c0 (id) de forma ascendente y solo quiero 10 lineas
    #Udemy_DF.select("*").where(Udemy_DF._c16 == 'English').sort(col("_c0").asc()).show(10)




