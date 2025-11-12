# ================================================================
# 📊 MILESTONE 4: DASHBOARDING & VISUALIZATION
# ================================================================

from pyspark.sql import functions as F
import os

# ------------------------------------------------
# STEP 0️⃣ — CONFIGURATION
# ------------------------------------------------

base_path = "/Volumes/workspace/default/hu_de_data/"

# Optional filters via Databricks widgets (useful for dashboards)
dbutils.widgets.text("start_date", "2017-01-01", "Start Date")
dbutils.widgets.text("end_date", "2018-12-31", "End Date")
dbutils.widgets.dropdown("state", "SP", ["SP", "RJ", "MG", "RS", "BA"], "Customer State")

start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
state = dbutils.widgets.get("state")

print(f"📅 Filtering from {start_date} to {end_date} for state {state}")

# ------------------------------------------------
# STEP 1️⃣ — LOAD DATA
# ------------------------------------------------

orders = spark.read.format("delta").load(os.path.join(base_path, "orders_repaired.delta"))
items = spark.read.format("delta").load(os.path.join(base_path, "order_items_with_seller_30d_on_time.delta"))
customers = spark.read.format("delta").load(os.path.join(base_path, "olist_customers_dataset_cleaned.delta"))
products = spark.read.format("delta").load(os.path.join(base_path, "olist_products_dataset_cleaned.delta"))
payments = spark.read.format("delta").load(os.path.join(base_path, "olist_order_payments_dataset_cleaned.delta"))

# ------------------------------------------------
# STEP 2️⃣ — BUILD BASE FACT TABLE
# ------------------------------------------------

fact_orders = (
    orders.join(items, "order_id")
          .join(customers, "customer_id")
          .join(products, "product_id", "left")
          .join(payments, "order_id", "left")
          .withColumn("revenue", F.col("price") + F.col("freight_value"))
          .withColumn("delivery_delay_days", F.datediff(F.col("delivered_ts_repaired"), F.col("order_estimated_delivery_date")))
          .withColumn("order_month", F.date_trunc("month", F.col("purchase_ts_repaired")))
)

# Apply filters from widgets
filtered = (
    fact_orders.filter(
        (F.col("purchase_ts_repaired") >= F.lit(start_date)) &
        (F.col("purchase_ts_repaired") <= F.lit(end_date)) &
        (F.col("customer_state") == F.lit(state))
    )
)

filtered.createOrReplaceTempView("filtered_orders")
print("✅ Created SQL view: filtered_orders")

# ------------------------------------------------
# STEP 3️⃣ — KPI 1: TOTAL REVENUE
# ------------------------------------------------
kpi_query = """
SELECT 
    ROUND(SUM(price + freight_value), 2) AS total_revenue
FROM filtered_orders
"""
kpi_df = spark.sql(kpi_query)
kpi_df.createOrReplaceTempView("kpi_total_revenue")
print("✅ KPI: Total revenue view created.")

# ------------------------------------------------
# STEP 4️⃣ — BAR CHART: TOP 10 CATEGORIES BY REVENUE
# ------------------------------------------------
bar_query = """
SELECT 
    product_category_name,
    ROUND(SUM(price + freight_value), 2) AS total_revenue
FROM filtered_orders
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 10
"""
bar_df = spark.sql(bar_query)
bar_df.createOrReplaceTempView("bar_top_categories")
print("✅ Bar chart view created.")

# ------------------------------------------------
# STEP 5️⃣ — LINE CHART: MONTHLY REVENUE TREND
# ------------------------------------------------
line_query = """
SELECT 
    date_format(order_month, 'yyyy-MM') AS month,
    ROUND(SUM(price + freight_value), 2) AS monthly_revenue
FROM filtered_orders
GROUP BY month
ORDER BY month
"""
line_df = spark.sql(line_query)
line_df.createOrReplaceTempView("line_revenue_trend")
print("✅ Line chart view created.")

# ------------------------------------------------
# STEP 6️⃣ — PIE CHART: PAYMENT METHOD SHARE
# ------------------------------------------------
pie_query = """
SELECT 
    payment_type,
    ROUND(SUM(payment_value), 2) AS total_payment
FROM filtered_orders
GROUP BY payment_type
ORDER BY total_payment DESC
"""
pie_df = spark.sql(pie_query)
pie_df.createOrReplaceTempView("pie_payment_share")
print("✅ Pie chart view created.")

# ------------------------------------------------
# STEP 7️⃣ — BOX PLOT: DELIVERY DELAY DISTRIBUTION
# ------------------------------------------------
box_query = """
SELECT 
    product_category_name,
    delivery_delay_days
FROM filtered_orders
WHERE delivery_delay_days IS NOT NULL
"""
box_df = spark.sql(box_query)
box_df.createOrReplaceTempView("box_delivery_delay")
print("✅ Box plot view created.")

# ------------------------------------------------
# STEP 8️⃣ — SAVE VISUALIZATION DATASETS AS DELTA
# ------------------------------------------------
kpi_df.write.mode("overwrite").format("delta").save(os.path.join(base_path, "viz_kpi_total_revenue.delta"))
bar_df.write.mode("overwrite").format("delta").save(os.path.join(base_path, "viz_bar_top_categories.delta"))
line_df.write.mode("overwrite").format("delta").save(os.path.join(base_path, "viz_line_revenue_trend.delta"))
pie_df.write.mode("overwrite").format("delta").save(os.path.join(base_path, "viz_pie_payment_share.delta"))
box_df.write.mode("overwrite").format("delta").save(os.path.join(base_path, "viz_box_delivery_delay.delta"))

print("\n💾 Visualization tables saved as Delta files.")
print("🎉 Milestone 4 completed — Dashboard views ready to visualize.")

# ------------------------------------------------
# STEP 9️⃣ — (Optional) VERIFY RESULTS
# ------------------------------------------------
spark.sql("SELECT * FROM kpi_total_revenue").show()
spark.sql("SELECT * FROM bar_top_categories LIMIT 5").show()
spark.sql("SELECT * FROM line_revenue_trend LIMIT 5").show()
spark.sql("SELECT * FROM pie_payment_share LIMIT 5").show()
spark.sql("SELECT * FROM box_delivery_delay LIMIT 5").show()
