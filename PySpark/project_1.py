
# Import the SparkSession class, which is the entry point for working with Spark.
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, BooleanType, DecimalType

# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("Project_1")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://rev1-249954438267-us-east-1-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("order_date", StringType()),
    StructField("ship_date", StringType()),
    StructField("quantity", StringType()),
    StructField("unit_price", StringType()),
    StructField("discount_pct", StringType()),
    StructField("total_amount", StringType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType())
])

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("cost", StringType()),
    StructField("stock_quantity", IntegerType()),
    StructField("weight_kg", FloatType()),
    StructField("created_date", StringType()),
    StructField("is_active", StringType())
])

customers_schema = StructType([
    StructField('customer_id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('phone', StringType()),
    StructField('signup_date', DateType()),
    StructField('country', StringType()),
    StructField('state', StringType()),
    StructField('postal_code', StringType()),
    StructField('is_active', BooleanType()),
    StructField('loyalty_points', IntegerType())
])


# Read the dataset from the csv file stored in S3
# and load it into a Spark DataFrame.
orders_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("s3://rev1-249954438267-us-east-1-an/orders.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .schema(products_schema)
    .csv("s3://rev1-249954438267-us-east-1-an/products.csv")
)

customers_df = (
    spark.read
    .option("header", True)
    .schema(customers_schema)
    .csv("s3://rev1-249954438267-us-east-1-an/customers.csv")
)

# Display the DataFrame's schema (column names and data types)
# to verify the data was loaded correctly.
orders_df.printSchema()
products_df.printSchema()
customers_df.printSchema()


# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")

# Remove duplicate customer IDs
customers_df_clean = customers_df.dropDuplicates(["customer_id"])

# Trim all string columns and convert blank strings to null
for field in customers_df_clean.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed_value = F.trim(F.col(field.name))

        customers_df_clean = customers_df_clean.withColumn(
            field.name,
            F.when(
                trimmed_value == "",
                None
            ).otherwise(trimmed_value)
        )

# Validate and normalize email addresses
email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

customers_df_clean = customers_df_clean.withColumn(
    "email",
    F.when(
        F.col("email").rlike(email_pattern),
        F.lower(F.col("email"))
    )
)

# Remove non-digit characters from phone numbers
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    F.regexp_replace(F.col("phone"), r"\D", "")
)

# Keep phone numbers containing between 10 and 15 digits
customers_df_clean = customers_df_clean.withColumn(
    "phone",
    F.when(
        F.length(F.col("phone")).between(10, 15),
        F.col("phone")
    )
)

# Replace negative loyalty points with zero
customers_df_clean = customers_df_clean.withColumn(
    "loyalty_points",
    F.when(
        F.col("loyalty_points") < 0,
        F.lit(0)
    ).otherwise(F.col("loyalty_points"))
)

#clean country 
customers_df_clean = customers_df_clean.withColumn(
    "country",
    F.when(
        F.lower(F.col("country")).isin(
            "usa",
            "u.s.a.",
            "united states",
            "us"
        ),
        F.lit("USA")
    ).when(
        F.lower(F.col("country")) == "canada",
        F.lit("Canada")
    ).when(
        F.lower(F.col("country")) == "mexico",
        F.lit("Mexico")
    ).otherwise(F.col("country"))
)

# cap states
customers_df_clean = customers_df_clean.withColumn(
    "state",
    F.upper(F.trim(F.col("state")))
)


# Remove records missing required customer information
customers_df_clean = customers_df_clean.dropna()

# clean products before orders, products is parent table
# Trim string columns and change blanks to NULL

products_df_clean = products_df

for field in products_df_clean.schema.fields:
    if isinstance(field.dataType, StringType):
        trimmed = F.trim(F.col(field.name))

        products_df_clean = products_df_clean.withColumn(
            field.name,
            F.when(trimmed == "", F.lit(None))
            .otherwise(trimmed)
        )

# Validate product ID
products_df_clean = products_df_clean.filter(
    F.col("product_id").rlike(r"^P\d{4}$")
)

# Remove duplicates
products_df_clean = products_df_clean.dropDuplicates(["product_id"])

# Clean price and cost
for column_name in ["price", "cost"]:
    products_df_clean = products_df_clean.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

    # Convert placeholder strings to actual NULL
    products_df_clean = products_df_clean.withColumn(
        column_name,
        F.when(
            F.lower(F.col(column_name)).isin(
                "", "null", "n/a", "na", "none", "?"
            ),
            F.lit(None)
        ).otherwise(F.col(column_name))
    )

    # Remove a leading dollar sign
    products_df_clean = products_df_clean.withColumn(
        column_name,
        F.regexp_replace(F.col(column_name), r"^\$", "")
    )

    # Convert decimal comma: 399,99 -> 399.99
    products_df_clean = products_df_clean.withColumn(
        column_name,
        F.when(
            F.col(column_name).rlike(r"^-?\d+,\d{2}$"),
            F.regexp_replace(F.col(column_name), ",", ".")
        ).otherwise(F.col(column_name))
    )

    # Safely cast malformed values to NULL
    products_df_clean = products_df_clean.withColumn(
        column_name,
        F.expr(
            f"try_cast(`{column_name}` AS DECIMAL(10,2))"
        )
    )

# Remove invalid numeric values
products_df_clean = products_df_clean.filter(
    (F.col("price") > 0)
    & (F.col("cost") >= 0)
    & (F.col("stock_quantity") >= 0)
)

# Standardize dates
products_df_clean = products_df_clean.withColumn(
    "created_date",
    F.regexp_replace(
        F.col("created_date"),
        "/",
        "-"
    )
)

products_df_clean = products_df_clean.withColumn(
    "created_date",
    F.to_date(
        F.try_to_timestamp(
            F.col("created_date"),
            F.lit("yyyy-MM-dd")
        )
    )
)

# Standardize boolean values
products_df_clean = products_df_clean.withColumn(
    "is_active",
    F.when(
        F.lower(F.col("is_active")).isin("true", "yes", "y"),
        F.lit(True)
    ).when(
        F.lower(F.col("is_active")).isin("false", "no", "n"),
        F.lit(False)
    )
)

# Drop rows missing required fields
products_df_clean = products_df_clean.dropna(
    subset=[
        "product_id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_quantity",
        "created_date",
        "is_active"
    ]
)


# clean orders.scv
orders_df_clean = orders_df

# Trim string values and convert blanks/"NULL" to actual null
for field in orders_df_clean.schema.fields:
    trimmed = F.trim(F.col(field.name))

    orders_df_clean = orders_df_clean.withColumn(
        field.name,
        F.when(
            F.lower(trimmed).isin("", "null"),
            F.lit(None)
        ).otherwise(trimmed)
    )

# Remove duplicate order IDs
orders_df_clean = orders_df_clean.dropDuplicates(["order_id"])

# Validate order ID
orders_df_clean = orders_df_clean.withColumn(
    "order_id",
    F.when(
        F.col("order_id").rlike(r"^\d+$"),
        F.col("order_id").cast(IntegerType())
    )
)

# Validate customer ID
orders_df_clean = orders_df_clean.withColumn(
    "customer_id",
    F.when(
        F.col("customer_id").rlike(r"^\d+$"),
        F.col("customer_id").cast(IntegerType())
    )
)

# Validate product ID format
orders_df_clean = orders_df_clean.filter(
    F.col("product_id").rlike(r"^P\d{4}$")
)

# clean quantity
orders_df_clean = orders_df_clean.withColumn(
    "quantity",
    F.col("quantity").cast(IntegerType())
)

orders_df_clean = orders_df_clean.filter(
    F.col("quantity") > 0
)

# clean unit price
orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", r"\$", "")
)

orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    F.expr("try_cast(unit_price AS DECIMAL(10,2))")
)

orders_df_clean = orders_df_clean.filter(
    F.col("unit_price") >= 0
)

#clean total amount
orders_df_clean = orders_df_clean.withColumn(
    "total_amount",
    F.expr("try_cast(total_amount AS DECIMAL(10,2))")
)

orders_df_clean = orders_df_clean.filter(
    F.col("total_amount") >= 0
)

# clean dates
for column_name in ["order_date", "ship_date"]:
    orders_df_clean = orders_df_clean.withColumn(
        column_name,
        F.regexp_replace(F.col(column_name), "/", "-")
    )

    orders_df_clean = orders_df_clean.withColumn(
        column_name,
        F.coalesce(
            F.to_date(
                F.try_to_timestamp(
                    F.col(column_name),
                    F.lit("yyyy-MM-dd")
                )
            ),
            F.to_date(
                F.try_to_timestamp(
                    F.col(column_name),
                    F.lit("MM-dd-yyyy")
                )
            )
        )
    )

orders_df_clean = orders_df_clean.withColumn(
    "discount_pct",
    F.regexp_replace(
        F.col("discount_pct"),
        "%",
        ""
    )
)

orders_df_clean = orders_df_clean.withColumn(
    "discount_pct",
    F.expr(
        "try_cast(discount_pct AS DECIMAL(5,2))"
    )
)

orders_df_clean = orders_df_clean.filter(
    F.col("discount_pct").between(0, 100)
)

orders_df_clean = orders_df_clean.withColumn(
    "calculated_total",
    F.round(
        F.col("quantity")
        * F.col("unit_price")
        * (
            F.lit(1)
            - F.col("discount_pct") / F.lit(100)
        ),
        2
    )
)
orders_df_clean = (
    orders_df_clean
    .drop("total_amount")
    .withColumnRenamed(
        "calculated_total",
        "total_amount"
    )
)


orders_df_clean = orders_df_clean.dropna()


# after all the data cleaning validate results by comparison of raw data
raw_customer_count = customers_df.count()
clean_customer_count = customers_df_clean.count()

raw_product_count = products_df.count()
clean_product_count = products_df_clean.count()

raw_order_count = orders_df.count()
clean_order_count = orders_df_clean.count()

print("Customers:", raw_customer_count, clean_customer_count)
print("Products:", raw_product_count, clean_product_count)
print("Orders:", raw_order_count, clean_order_count)

# left semi join to remove orders whose customer or product id does not exist

valid_orders_df = (
    orders_df_clean
    .join(
        customers_df_clean.select("customer_id"),
        on="customer_id",
        how="left_semi"
    )
    .join(
        products_df_clean.select("product_id"),
        on="product_id",
        how="left_semi"
    )
)

# joined table
sales_curated_df = (
    valid_orders_df.alias("o")
    .join(
        customers_df_clean.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "inner"
    )
    .join(
        products_df_clean.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "inner"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.order_date"),
        F.col("o.ship_date"),
        F.col("o.customer_id"),
        F.concat_ws(
            " ",
            F.col("c.first_name"),
            F.col("c.last_name")
        ).alias("customer_name"),
        F.col("c.country"),
        F.col("c.state"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.brand"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.discount_pct"),
        F.col("o.total_amount"),
        F.col("p.cost"),
        F.round(
            F.col("o.total_amount")
            - (F.col("o.quantity") * F.col("p.cost")),
            2
        ).alias("profit"),
        F.col("o.payment_method"),
        F.col("o.order_status")
    )
)

# calculate profit margins
margin_df = (
    sales_curated_df
    .select(
        "order_date",
        "product_name",
        "quantity",
        "cost",
        "unit_price",
        "total_amount",
        "profit"
    )
    .withColumn(
        "profit_margin_pct",
        F.round(
            (F.col("profit") / F.col("total_amount")) * 100,
            2
        )
    )
)

# sales by categories
category_summary_df = (
    sales_curated_df
    .groupBy("category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.sum("profit"), 2).alias("profit")
    )
)

# sales by payment method
payment_summary_df = (
    sales_curated_df
    .groupBy("payment_method")
    .agg(
        F.count("*").alias("orders"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.sum("profit"), 2).alias("profit")
    )
)

# Write the DataFrame as an Iceberg table.
(
    customers_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)
# Write the DataFrame as an Iceberg table.
(
    products_df_clean.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.products"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

valid_orders_df.writeTo(
    "glue_catalog.iceberg_catalog_db.orders"
).using("iceberg").createOrReplace()

sales_curated_df.writeTo(
    "glue_catalog.iceberg_catalog_db.sales_curated"
).using("iceberg").createOrReplace()

margin_df.writeTo(
    "glue_catalog.iceberg_catalog_db.margin"
).using("iceberg").createOrReplace()

category_summary_df.writeTo(
    "glue_catalog.iceberg_catalog_db.category_summary"
).using("iceberg").createOrReplace()

payment_summary_df.writeTo(
    "glue_catalog.iceberg_catalog_db.payment_summary"
).using("iceberg").createOrReplace()

# Query the newly created Iceberg table to verify that the
# data was written successfully.
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.customers
LIMIT 10
""").show()

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.products
LIMIT 10
""").show()

spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.orders
LIMIT 10
""").show()

print("Sales curated table")
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.sales_curated
ORDER BY order_date
""").show(50, truncate=False)

print("Profit Margins")
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.margin
ORDER BY profit_margin_pct desc
""").show(50, truncate=False)

print("Category summaries")
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.category_summary
ORDER BY units_sold DESC
LIMIT 10
""").show(truncate=False)

print("Payment summaries")
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.payment_summary
ORDER BY orders DESC
LIMIT 10
""").show(truncate=False)
# Stop the Spark session and release cluster resources.
spark.stop()