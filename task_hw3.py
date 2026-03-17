from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# 1. Створюємо сесію Spark
spark = (
    SparkSession.builder.appName("Spark_HW3")
    .master("spark://192.168.64.4:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Завантажуємо датасети
users_df = spark.read.csv("data/users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("data/purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

# 2. Очищення даних
users_clean = users_df.dropna()
purchases_clean = purchases_df.dropna()
products_clean = products_df.dropna()

print("Очищені дані (приклад х5)")
users_clean.show(5)
purchases_clean.show(5)
products_clean.show(5)

# Об'єднання таблиць --Join--
# - об'єднуємо покупки з продуктами -→ отримуємо ціну та категорію
# - об'єднуємо з користувачами -→ отримуємо вік
joined_df = purchases_clean.join(products_clean, "product_id").join(
    users_clean, "user_id"
)

# Додаємо колонку загальної вартості однієї покупки (ціна * кількість)
joined_df = joined_df.withColumn("total_price", col("price") * col("quantity"))
print("Повна таблиця після Join:")
joined_df.show(10)

# 3. Загальна сума покупок за кожною категорією
category_totals = joined_df.groupBy("category").agg(
    sum("total_price").alias("sum_spent")
)
print("Загальна сума за категоріями:")
category_totals.show()

# 4. Сума покупок за категоріями для віку 18-25
young_users_df = joined_df.filter((col("age") >= 18) & (col("age") <= 25))
young_category_totals = young_users_df.groupBy("category").agg(
    sum("total_price").alias("sum_spent_18_25")
)
print("Сума покупок для вік. категорії 18-25:")
young_users_df.show(10)

# 5. Частка покупок за кожною категорією (відсоток від загальних витрат групи 18-25)
total_young_spent = young_users_df.select(sum("total_price")).collect()[0][0]

# Додаємо колонку з відсотком та округлюємо до другого знака після коми
young_category_analysis = young_category_totals.withColumn(
    "percentage", round((col("sum_spent_18_25") / total_young_spent) * 100, 2)
)

# 6. Вибір 3 категорій з найвищим відсотком
top_3_categories = young_category_analysis.orderBy(col("percentage").desc()).limit(3)

print("Топ-3 категорії для віку 18-25:")
top_3_categories.show()

spark.stop()
