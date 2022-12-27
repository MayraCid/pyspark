from pyspark.context import SparkContext
sc = SparkContext('local', 'test')

#In Python
# Create an RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])

# Use map and reduceByKey transformations with their lambda
# expressions to aggregate and then compute average
agesRDD = (dataRDD
 .map(lambda x: (x[0], (x[1], 1))) #map permite aplicar una función sobre un objeto iterable
 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) #reducebykey retorna un RDD es para una transofrmación
 .map(lambda x: (x[0], x[1][0]/x[1][1])))

#PARA VER UN RDD SE UTILIZA COLLECT.() ¡¡SIMEPRE!!
print(dataRDD.collect())
print("separación")
print(agesRDD.collect())