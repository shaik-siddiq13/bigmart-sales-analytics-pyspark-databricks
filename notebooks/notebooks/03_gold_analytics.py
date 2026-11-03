gold_df = spark.table(
    "bigmart_catalog.bigmark_schema.silver_bigmart_sales"
)

display(gold_df)
print("Total records:", gold_df.count())
from pyspark.sql.functions import sum as spark_sum 
sales_by_item_type = gold_df.groupBy("Item_Type").agg(
    spark_sum("Item_Outlet_Sales").alias("Total_Sales")
)

display(sales_by_item_type)
from pyspark.sql.functions import col 

sales_by_item_type_sorted = sales_by_item_type.orderBy(
    col("Total_Sales").desc()
)

display(sales_by_item_type_sorted)
from pyspark.sql.functions import sum as spark_sum 

sales_by_outlet = gold_df.groupBy(
    "Outlet_Identifier"
).agg(
    spark_sum("Item_Outlet_Sales").alias("Totak_Sales")
)

display(sales_by_outlet)
from pyspark.sql.functions import col 

sales_by_outlet_sorted = sales_by_outlet.orderBy(
    col("Total_Sales").desc()
)

display("sales_by_outlet_sorted")
sales_by_outlet_sorted = sales_by_outlet.orderBy(
    col("Totak_Sales").desc()
)

sales_by_outlet_sorted.show()
from pyspark.sql.functions import avg

avg_sales_by_outlet_type = gold_df.groupBy(
    "Outlet_Type"
).agg(
    avg("Item_Outlet_Sales").alias("Average_Sales")
)

display(avg_sales_by_outlet_type)
from pyspark.sql.functions import sum as spark_sum

sales_by_location = gold_df.groupBy(
    "Outlet_Location_Type"
).agg(
    spark_sum("Item_Outlet_Sales").alias("Total_sales")
)

display(sales_by_location)
from pyspark.sql.functions import sum as spark_sum, col

top_products = gold_df.groupBy(
    "Item_Identifier"
).agg(
    spark_sum("Item_Outlet_Sales").alias("Total_Sales")
)

top_10_products = top_products.orderBy(
    col("Total_Sales").desc()
).limit(10)

display(top_10_products)
sales_by_item_type_sorted.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "bigmart_catalog.bigmark_schema.sales_by_item_type"
    )
gold_item_sales_df = spark.table(
    "bigmart_catalog.bigmark_schema.sales_by_item_type"
)

display(gold_item_sales_df)

print("Total Categories:", gold_item_sales_df.count())
# 1. Sales by Outlet
sales_by_outlet_sorted.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bigmart_catalog.bigmark_schema.gold_sales_by_outlet")


# 2. Average Sales by Outlet Type
avg_sales_by_outlet_type.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bigmart_catalog.bigmark_schema.gold_avg_sales_by_outlet_type")


# 3. Sales by Outlet Location
sales_by_location.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bigmart_catalog.bigmark_schema.gold_sales_by_location")


# 4. Top 10 Products
top_10_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bigmart_catalog.bigmark_schema.gold_top_10_products")
gold_tables = [
    "sales_by_item_type",
    "gold_sales_by_outlet",
    "gold_avg_sales_by_outlet_type",
    "gold_sales_by_location",
    "gold_top_10_products"
]

for table_name in gold_tables:

    table_df = spark.table(
        f"bigmart_catalog.bigmark_schema.{table_name}"
    )

    print(table_name, ":", table_df.count(), "records")
