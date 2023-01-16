
# Podemos ver los elementos nulos, ya sea por columna o id
Udemy_DF_year.filter(Udemy_DF_year.instructor_url.isNull()).show(4)
print(f"Execution time nulls files: {time.time() - start_time}")

# Eliminando los datos nulos y definiendo un nuevo DataFrame
New_Udemy_DF = Udemy_DF_year.na.drop()
print(f"Execution time drop null files: {time.time() - start_time}")

# Número de filas
filas = New_Udemy_DF.count()
print(f"El número de filas es : {filas}")
# Número de columnas
Columnas = len(New_Udemy_DF.columns)
print(f"El número de columnas es : {Columnas}")
print(f"Execution time number files and columns: {time.time() - start_time}")


# Datos de algunas columnas
Udemy_DF_year.describe(['num_subscribers']).show()
Udemy_DF_year.describe(['avg_rating']).show()
Udemy_DF_year.describe(['num_reviews']).show()
Udemy_DF_year.describe(['num_comments']).show()
Udemy_DF_year.describe(['num_lectures']).show()
print(f"Execution time for describe: {time.time() - start_time}")

# Ahora vemos categorias y cuantos hay por cada una en orden descendiente
df3 = New_Udemy_DF.groupBy("category").count().orderBy("count", ascending = False)
df3.show()
print(f"Execution time searching categorys: {time.time() - start_time}")

# Ahora vamos a graficar pero para ello hay que pasar a un RDD
dfx = df3.select('category')
dfy = df3.select('count')

x_rdd = dfx.rdd.map(lambda x: x[0])
y_rdd = dfy.rdd.map(lambda x: x[0])
print(f"Execution time transform RDD: {time.time() - start_time}")

x = x_rdd.collect()
y = y_rdd.collect()

# plt.xlabel('Número de cursos')
# plt.ylabel('categorías')
# plt.barh(x,y)
# plt.show()
# print(f"Execution time fisrt graph: {time.time() - start_time}")


# Veamos las subcategorías pero solo de la categoria = development!!! esto importantisimo
df4 = New_Udemy_DF[New_Udemy_DF.category == "Development"]
print(f"Execution time searching Development: {time.time() - start_time}")


df5 = df4.groupBy("subcategory").count().orderBy("count", ascending=False)
df5.show()
print(f"Execution time Development: {time.time() - start_time}")

# Veamos la grafica de subcategorias de la categoría development
df1x = df5.select('subcategory')
df1y = df5.select('count')

# Ahora vamos a graficar pero para ello hay que pasar a un RDD
x1_rdd = df1x.rdd.map(lambda x: x[0])
y1_rdd = df1y.rdd.map(lambda x: x[0])
print(f"Execution time transform RDD: {time.time() - start_time}")

x1 = x1_rdd.collect()
y1 = y1_rdd.collect()

# plt.xlabel('Número de cursos')
# plt.ylabel('Development')
# plt.barh(x1,y1)
# plt.show()
# print(f"Execution time graph 2: {time.time() - start_time}")

# Número de cursos por año
df6 = New_Udemy_DF.groupBy("year_published").count().orderBy("year_published", ascending=False)
df6.show()
print(f"Execution time order year: {time.time() - start_time}")

# Presentamos una gráfica de la cantidad de cursos por año
dfx2 = df6.select("year_published")
dfy2 = df6.select("count")

# Vamos a gráficar pero para ello hay que pasar a un RRD
x2_rdd = dfx2.rdd.map(lambda x: x[0])
y2_rdd = dfy2.rdd.map(lambda x: x[0])
print(f"Execution time transform RDD: {time.time() - start_time}")

x2 = x2_rdd.collect()
y2 = y2_rdd.collect()

# plt.xlabel('Año')
# plt.ylabel('Número de cursos ')
# plt.plot(x2,y2)
# plt.show()
# print(f"Execution time: {time.time() - start_time}")
