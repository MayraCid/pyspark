#Vamos hacer primero una SparkSession
from pyspark.sql import SparkSession

if __name__ == "__main__":
    print('Iniciando lectura ...')

    spark = SparkSession \
        .builder \
        .appName("QuijoteDF") \
        .master("local[*]") \
        .getOrCreate()

    #Va a leer el texto y el contenido estará en DataFrame
    file_DF = spark.read.text("C://Users//mayra.cid//Documents//El_Quijote//el_quijote.txt")

    print("imprimiendo DataFrame ...")
    #Toma un núm de líneas y los pone como valores en un DataFrame
    #quijote_lines = file_DF.show(5, truncate=False, vertical=False)
    quijote_lines = file_DF.first() #Regresa la primera linea como ROW
    #quijote_lines = file_DF.head() #te imprime las n primeras lineas q digas como ROW (array PEQUEÑO)
    #quijote_lines = file_DF.take(1) #Regresa los primeros renglones como una lista de ROW

    print(quijote_lines)

    #Como es un DataFrame el count() solo cuenta las lineas
    #del DataFrame total, i.e. ya paso el archivo a un DataFrame
    quijote_total = file_DF.count()
    print("El número total de lineas en el archivo es: ", quijote_total)
    print("listo")