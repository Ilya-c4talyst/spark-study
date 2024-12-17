from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, col, lit
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("Пример PySpark") \
    .getOrCreate()

data = [("Иван", 29), ("Мария", 24), ("Петр", 35)]
columns = ["Имя", "Возраст"]

df = spark.createDataFrame(data, columns)

df_replaced = df.withColumn("Имя", when(col("Имя") == "Мария", "Марина").otherwise(col("Имя")))
df_replaced1 = df.withColumn("Возраст", lit(30))
# df = df.withColumn("Height",
#     when(col("Name") == "Ilya", 175)
#     .when(col("Name") == "Roman", 170)
#     .otherwise(col("Height")))


df_replaced.show()
df_replaced1.show()

print("Исходный DataFrame:")
df.show()

df_filtered = df.filter(df.Возраст > 30)
print("Отфильтрованный DataFrame (Возраст > 30):")
df_filtered.show()


df_grouped = df.groupBy("Имя").count()
print("Группировка и подсчет:")
df_grouped.show()

# df_csv = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
# df_csv.show()

# Запись DataFrame в CSV файл (раскомментируйте, чтобы записать)
# df.write.csv("path/to/output.csv", header=True)


def greet(name):
    return f"Привет, {name}!"


greet_udf = udf(greet, StringType())

df_greeted = df.withColumn("Приветствие", greet_udf(df.Имя))
print("DataFrame с приветствием:")
df_greeted.show()

spark.stop()

