from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import year
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col,isnan, when, count, desc
from pyspark.sql.types import IntegerType,BooleanType,DateType
import matplotlib.pyplot as plt
from pyspark.rdd import RDD
import time



if __name__ == "__main__":
    print('Iniciando lectura ...')

    spark = SparkSession \
        .builder \
        .appName("UdemyDF3") \
        .master("local[*]") \
        .getOrCreate()

    #Defino antes mi schema
    schema = StructType() \
        .add("id", FloatType(), True) \
        .add("title", StringType(), True) \
        .add("is_paid", BooleanType(), True) \
        .add("price", FloatType(), True) \
        .add("headline", StringType(), True) \
        .add("num_subscribers", FloatType(), True) \
        .add("avg_rating", FloatType(), True) \
        .add("num_reviews", FloatType(), True) \
        .add("num_comments", FloatType(), True) \
        .add("num_lectures", FloatType(), True) \
        .add("content_length_min", FloatType(), True) \
        .add("published_time", TimestampType(), True) \
        .add("last_update_date", DateType(), True) \
        .add("category", StringType(), True) \
        .add("subcategory", StringType(), True) \
        .add("topic", StringType(), True) \
        .add("language", StringType(), True) \
        .add("course_url", StringType(), True) \
        .add("instructor_name", StringType(), True) \
        .add("instructor_url", StringType(), True)

    start_time = time.time()

    #Si no pones delimitadores vas a teneeener muchos problemas
    Udemy2_DF = spark.read.options(escape='"', header = 'True').option("multiline",'true').schema(schema) \
        .csv("C://Users//mayra.cid//Documents//Repositorios//Udemy_Dataset//Course_info.csv")

    print(f"Execution time read: {time.time() - start_time}")

    #print(Udemy2_DF.printSchema())
    #Udemy2_DF.show(5)

    #Cambio del tipo de dato
    Udemy2_DF = Udemy2_DF.withColumn("id", col("id").cast('int')) \
        .withColumn("num_subscribers", col("num_subscribers").cast('int')) \
        .withColumn("num_reviews", col("num_reviews").cast('int')) \
        .withColumn("num_comments", col("num_comments").cast('int')) \
        .withColumn("num_lectures", col("num_lectures").cast('int')) \
        .withColumn("content_length_min", col("content_length_min").cast('int'))

    print(f"Execution time data chage: {time.time() - start_time}")


    #print(Udemy2_DF.printSchema())
    #Udemy2_DF.show(5)

    #Agregamos la columna de año
    Udemy_DF_year = Udemy2_DF.withColumn('year_published', year(Udemy2_DF.published_time))

    print(f"Execution time adding new column: {time.time() - start_time}")

    #print(Udemy_DF_year.printSchema())
    #Udemy_DF_year.show(5)

    #Número de filas
    filas = Udemy_DF_year.count()
    print(f"El número de filas es : {filas}")
    #Número de columnas
    Columnas = len(Udemy_DF_year.columns)
    print(f"El número de columnas es : {Columnas}")

    print(f"Execution time number files and columns: {time.time() - start_time}")

    #Vemos cuántos valores nulos hay e cada columna
    Columns = ["id", "title", "is_paid", "price", "headline", "num_subscribers",
               "avg_rating", "num_reviews","num_comments","num_lectures",
               "content_length_min", "published_time", "last_update_date", "category",
               "subcategory", "topic", "language", "course_url", "instructor_name",
               "instructor_url", "year_published"]
    Udemy_DF_year.select([count(when(col(c).isNull(), c)).alias(c) for c in Columns]).show()

    print(f"Execution time looking for nulls: {time.time() - start_time}")

    #Podemos ver los elementos nulos, ya sea por columna o id
    Udemy_DF_year.filter(Udemy_DF_year.instructor_url.isNull()).show(4)
    #Udemy_DF_year.filter(Udemy_DF_year.id == '205502').show()

    print(f"Execution time nulls files: {time.time() - start_time}")

    #Eliminando los datos nulos y definiendo un nuevo DataFrame
    #También se puede eliminar por columna con definiendo subset
    New_Udemy_DF = Udemy_DF_year.na.drop()

    print(f"Execution time drop null files: {time.time() - start_time}")

    #Número de filas
    filas = New_Udemy_DF.count()
    print(f"El número de filas es : {filas}")
    # Número de columnas
    Columnas = len(New_Udemy_DF.columns)
    print(f"El número de columnas es : {Columnas}")

    print(f"Execution time number files and columns: {time.time() - start_time}")

    #Ahora vemos categorias y cuantos hay por cada una en orden descendiente
    df3 = New_Udemy_DF.groupBy("category").count().orderBy("count", ascending = False)
    df3.show()

    print(f"Execution time searching categorys: {time.time() - start_time}")

    #Ahora vamos a graficar pero para ello hay que pasar a un RDD
    dfx = df3.select('category')
    dfy = df3.select('count')

    x_rdd = dfx.rdd.map(lambda x: x[0])
    y_rdd = dfy.rdd.map(lambda x: x[0])

    print(f"Execution time transform RDD: {time.time() - start_time}")

    x = x_rdd.collect()
    y = y_rdd.collect()

    #plt.xlabel('Número de cursos')
    #plt.ylabel('categorías')
    #plt.barh(x,y)
    #plt.show()

    print(f"Execution time fisrt graph: {time.time() - start_time}")

    #Veamos las subcategorías pero solo de la categoria = development!!! esto importantisimo
    df4 = New_Udemy_DF[New_Udemy_DF.category == "Development"]

    df5 = df4.groupBy("subcategory").count().orderBy("count", ascending=False)
    df5.show()

    print(f"Execution time Development: {time.time() - start_time}")

    #Veamos la grafica de subcategorias de la categoría development
    df1x = df5.select('subcategory')
    df1y = df5.select('count')

    #Ahora vamos a graficar pero para ello hay que pasar a un RDD
    x1_rdd = df1x.rdd.map(lambda x: x[0])
    y1_rdd = df1y.rdd.map(lambda x: x[0])

    x1 = x1_rdd.collect()
    y1 = y1_rdd.collect()

    print(f"Execution time transform RDD: {time.time() - start_time}")

    #plt.xlabel('Número de cursos')
    #plt.ylabel('Development')
    #plt.barh(x1,y1)
    #plt.show()

    print(f"Execution time graph 2: {time.time() - start_time}")

    #Número de cursos por año
    df6 = New_Udemy_DF.groupBy("year_published").count().orderBy("year_published", ascending=False)
    df6.show()

    print(f"Execution time order year: {time.time() - start_time}")

    #Presentamos una gráfica de la cantidad de cursos por año
    dfx2 = df6.select("year_published")
    dfy2 = df6.select("count")

    #Vamos a gráficar pero para ello hay que pasar a un RRD
    x2_rdd = dfx2.rdd.map(lambda x: x[0])
    y2_rdd = dfy2.rdd.map(lambda x: x[0])

    x2 = x2_rdd.collect()
    y2 = y2_rdd.collect()

    print(f"Execution time transform in RDD: {time.time() - start_time}")

    plt.xlabel('Año')
    plt.ylabel('Número de cursos ')
    plt.plot(x2,y2)
    plt.show()

    print(f"Execution time: {time.time() - start_time}")


    #Notas: el primero solo nos da un query más no un DF para ello ocupo lo que tengo arriba
    #category = New_Udemy_DF.select("category")\
     #   .groupBy("category")\
     #   .count()\
     #   .orderBy("count", ascending = False)\
     #   .show()

    #Para ver si algo es un dataframe ocupamos
    #print(isinstance(df4, DataFrame))







