from pyspark.sql import SparkSession

spark = SparkSession.builder .appName(
    "Продукты и Категории").getOrCreate()

products_data = [
    (1, "Продукт A"),
    (2, "Продукт B"),
    (3, "Продукт C"),
    (4, "Продукт D"),
    (5, "Продукт E")
]

categories_data = [
    (1, "Категория 1"),
    (2, "Категория 2"),
    (3, "Категория 3")
]

product_categories_data = [
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 1)
]

products_df = spark.createDataFrame(
    products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(
    categories_data, ["category_id", "category_name"])
product_categories_df = spark.createDataFrame(
    product_categories_data, ["product_id", "category_id"])


def get_product_category_pairs(products, categories, product_categories):
    """Соединяем датафреймы, чтобы получить пары."""
    product_category_pairs = products.join(
        product_categories, on="product_id", how="left"
        ).join(categories, on="category_id", how="left").select(
            "product_name", "category_name")

    """Продукты без категорий."""
    products_without_category = products.join(
        product_categories, on="product_id", how="left_anti"
        ).select("product_name")

    """Объединяем оба датафрейма."""
    result_df = product_category_pairs.union(
        products_without_category.withColumnRenamed(
            "product_name", "category_name"))

    return result_df


result = get_product_category_pairs(
    products_df, categories_df, product_categories_df)
result.show()
