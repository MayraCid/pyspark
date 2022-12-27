#Vamos hacer primero una SparkSession
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print('Iniciando lectura ...')

    spark = SparkSession \
        .builder \
        .appName("QuijoteRDD") \
        .master("local[*]") \
        .getOrCreate()

    #Ruta del archivo
    file_path = "C://Users//mayra.cid//Documents//El_Quijote//el_quijote.txt"

    #Ya esta leyendo el archivo en un RDD
    quijote_rdd = spark.sparkContext.textFile(file_path)

    print("imprimiendo")

    #El .collect() regresa una lista que contiene
    #todos los elementos del RDD, i.e. toma los
    #renglones del archivo e incluso los espacios
    #en blanco ('') como elementos de la lista

    print(quijote_rdd.collect())
    #print(quijote_rdd.first()) #Regresa la primera linea como ROW
    #print(quijote_rdd.head(n)) #te imprime las n primeras lineas q digas como ROW (SOLO array PEQUEÑO)
    print(quijote_rdd.take(2)) #Regresa los primeros renglones como una lista de ROW
    print("listo")

    print(quijote_rdd.count())