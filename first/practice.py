from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col


ss = SparkSession.builder.appName("First").getOrCreate()

# 1. Создать DF с информацией о студентах
values = [("Илья", 18, 95), ("Иван", 17, 85), ("Мария", 18, 63), ("Оля", 22, 53), ("Алекс", 23, 43),]
columns = ["Name", "Age", "Mark"]

df = ss.createDataFrame(values, columns)

df.show()

# 2. Замена значений
df = df.withColumn("Age", when(col("Name") == "Алекс", 20).otherwise(col("Age")))
df = df.withColumn("Mark", when(col("Name") == "Оля", 95).otherwise(col("Mark")))

df.show()

# Задание 3: Условная замена
# Используя метод when, замените оценки студентов:

# Если оценка меньше 60 — замените на "Неудовлетворительно".
# Если оценка от 60 до 80 — замените на "Удовлетворительно".
# Если оценка от 81 до 90 — замените на "Хорошо".
# Если оценка больше 90 — замените на "Отлично".

df = df.withColumn(
    "Mark",
    when(col("Mark") < 60, "Неудовлетворительно")
    .when((col("Mark") >= 60) & (col("Mark") <= 80), "Удовлетворительно")
    .when((col("Mark") >= 81) & (col("Mark") <= 90), "Хорошо")
    .when(col("Mark") > 90, "Отлично")

    .otherwise(col("Mark"))
)
df.show()

# Задание 4: Поменять местами
# Поменяйте местами оценки между студентами "Иван" и "Мария".
df = df.withColumn(
    "Mark",
    when(col("Name") == "Иван", df.filter(df["Name"] == "Мария").select("Mark").first().Mark)
    .when(col("Name") == "Мария", df.filter(df["Name"] == "Иван").select("Mark").first().Mark)

    .otherwise(col("Mark"))
)


df.show()

# Задание 5: Вывод информации
# Выведите DataFrame до и после всех замен.
# Выведите количество студентов с каждой оценкой (например, "Неудовлетворительно", "Удовлетворительно", и т.д.).
df_check = df.groupBy("Mark").count()
df_check.show()

ss.stop()