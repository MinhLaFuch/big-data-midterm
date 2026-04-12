"""
EXAMPLE: Analyzing a sales dataset using PySpark RDDs and DataFrames

sales.csv format:
  customer_id, date (dd/mm/yyyy), product, year, month, day

This is analogous to your Task 2 which analyzes baskets.csv.
The logic pattern is identical - just swap column names for your actual data.

Run with:
  spark-submit task2_example.py sales.csv
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import math
import os
import matplotlib.pyplot as plt

# SETUP
conf = SparkConf().setAppName("SalesAnalysis").setMaster("local[*]")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")  # suppress noisy Spark logs

input_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "baskets.csv")

# PART A: RDDs
# Never use DataFrames here - only RDD operations

raw_rdd = sc.textFile(input_path)

# Skip the header row
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

# Parse each line into a tuple
# (customer_id, date, product, year, month, day)
def parse_line(line):
    parts = line.split(",")
    return (
        parts[0].strip(),   # customer_id
        parts[1].strip(),   # date
        parts[2].strip(),   # product
        int(parts[3]),      # year
        int(parts[4]),      # month
        int(parts[5]),      # day
    )

parsed_rdd = data_rdd.map(parse_line)

# f1: Top 10 customers who bought the largest number of DISTINCT products
# Pattern: group by customer -> collect distinct products -> count -> sort
def f1(rdd, output_folder):
    result = (
        rdd
        .map(lambda r: (r[0], r[2]))            # (customer_id, product)
        .distinct()                              # remove duplicate (customer, product) pairs
        .map(lambda r: (r[0], 1))               # (customer_id, 1)
        .reduceByKey(lambda a, b: a + b)         # (customer_id, distinct_count)
        .sortBy(lambda r: r[1], ascending=False) # sort descending
        .take(10)                                # top 10
    )

    print("\n=== f1: Top 10 customers by distinct products ===")
    for customer, count in result:
        print(f"  Customer {customer}: {count} distinct products")

    # Save to folder
    sc.parallelize(result) \
      .map(lambda r: f"{r[0]},{r[1]}") \
      .saveAsTextFile(output_folder)

    # Visualize

    customers = [r[0] for r in result]
    counts    = [r[1] for r in result]
    plt.figure(figsize=(10, 5))
    plt.bar(customers, counts, color='steelblue')
    plt.title("Top 10 Customers by Distinct Products")
    plt.xlabel("Customer ID")
    plt.ylabel("Distinct Products")
    plt.tight_layout()
    plt.savefig("f1_chart.png")
    print("  Chart saved to f1_chart.png")

# f2: Number of shopping baskets (unique customer+date combos) per month
# Pattern: group by (customer, date) to find baskets, then count by month
def f2(rdd, output_folder):
    result = (
        rdd
        .map(lambda r: ((r[0], r[1]), r[4]))     # ((customer, date), month)
        .distinct()                               # one basket per customer per date
        .map(lambda r: (r[1], 1))                 # (month, 1)
        .reduceByKey(lambda a, b: a + b)          # (month, basket_count)
        .sortBy(lambda r: r[0])                   # sort by month
        .collect()
    )

    print("\n=== f2: Baskets per month ===")
    for month, count in result:
        print(f"  Month {month}: {count} baskets")

    sc.parallelize(result) \
      .map(lambda r: f"{r[0]},{r[1]}") \
      .saveAsTextFile(output_folder)

    months = [r[0] for r in result]
    counts = [r[1] for r in result]
    plt.figure(figsize=(10, 5))
    plt.plot(months, counts, marker='o', color='darkorange')
    plt.title("Shopping Baskets per Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Baskets")
    plt.xticks(months)
    plt.tight_layout()
    plt.savefig("f2_chart.png")
    print("  Chart saved to f2_chart.png")

# f3: Most frequent PAIR of products in each month
# Pattern: for each basket -> generate all pairs -> count by (month, pair) -> find max per month
def get_pairs(items):
    """Generate all unique 2-item combinations from a list"""
    items = sorted(set(items))  # sort so (A,B) and (B,A) are the same pair
    pairs = []
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            pairs.append((items[i], items[j]))
    return pairs

def f3(rdd, output_folder):
    # Step 1: group items into baskets (customer, date, month) -> [products]
    baskets = (
        rdd
        .map(lambda r: ((r[0], r[1], r[4]), r[2]))   # ((cust, date, month), product)
        .groupByKey()
        .mapValues(list)
    )

    # Step 2: for each basket, emit ((month, pair), 1)
    pairs_rdd = (
        baskets
        .flatMap(lambda r: [((r[0][2], pair), 1) for pair in get_pairs(r[1])])
        .reduceByKey(lambda a, b: a + b)   # count each (month, pair)
    )

    # Step 3: for each month find the max pair
    # key=month, value=(pair, count)
    by_month = pairs_rdd.map(lambda r: (r[0][0], (r[0][1], r[1])))
    most_frequent = (
        by_month
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .sortBy(lambda r: r[0])
        .collect()
    )

    print("\n=== f3: Most frequent product pair per month ===")
    for month, (pair, count) in most_frequent:
        print(f"  Month {month}: {pair} (count={count})")

    months = [r[0] for r in most_frequent]
    pairs  = [str(r[1][0]) for r in most_frequent]
    counts = [r[1][1] for r in most_frequent]

    plt.figure(figsize=(10, 5))
    plt.bar(months, counts, color='mediumpurple')

    plt.title("Most Frequent Product Pair per Month")
    plt.xlabel("Month")
    plt.ylabel("Pair Frequency")

    # label bars with pair names
    for i, txt in enumerate(pairs):
        plt.text(months[i], counts[i], txt, ha='center', va='bottom', fontsize=8, rotation=45)

    plt.tight_layout()
    plt.savefig("f3_chart.png")
    print("  Chart saved to f3_chart.png")

    sc.parallelize(most_frequent) \
      .map(lambda r: f"{r[0]},{r[1][0]},{r[1][1]}") \
      .saveAsTextFile(output_folder)

# f4: Customer with most IRREGULAR purchasing behavior
# Irregularity = largest std deviation of items purchased per day
# Pattern: group by (customer, date) -> count items -> compute stddev per customer
def stddev(values):
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    return math.sqrt(variance)

def f4(rdd, output_folder):
    # Step 1: count items per (customer, date)
    items_per_day = (
        rdd
        .map(lambda r: ((r[0], r[1]), 1))          # ((customer, date), 1)
        .reduceByKey(lambda a, b: a + b)            # ((customer, date), item_count)
        .map(lambda r: (r[0][0], r[1]))             # (customer, item_count)
        .groupByKey()
        .mapValues(list)
    )

    # Step 2: compute std deviation per customer
    customer_stddev = (
        items_per_day
        .map(lambda r: (r[0], stddev(r[1])))
        .sortBy(lambda r: r[1], ascending=False)
    )

    most_irregular = customer_stddev.first()

    print("\n=== f4: Most irregular customer ===")
    print(f"  Customer {most_irregular[0]} with std dev = {most_irregular[1]:.4f}")

    top_10 = customer_stddev.take(10)

    customers = [r[0] for r in top_10]
    stds      = [r[1] for r in top_10]

    plt.figure(figsize=(10, 5))
    plt.bar(customers, stds, color='tomato')

    plt.title("Top 10 Most Irregular Customers")
    plt.xlabel("Customer ID")
    plt.ylabel("Std Deviation of Daily Purchases")
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.savefig("f4_chart.png")
    print("  Chart saved to f4_chart.png")

    customer_stddev.map(lambda r: f"{r[0]},{r[1]:.4f}") \
                   .saveAsTextFile(output_folder)

# Run all functions
f1(parsed_rdd, "output/f1")
f2(parsed_rdd, "output/f2")
f3(parsed_rdd, "output/f3")
f4(parsed_rdd, "output/f4")


# PART B: DataFrames
# Re-implement the same functions using DataFrame API

df = spark.read.csv(input_path, header=True, inferSchema=True)
# Expected columns: customer_id, date, product, year, month, day
df = df.withColumnRenamed("Member_number", "customer_id") \
       .withColumnRenamed("Date", "date") \
       .withColumnRenamed("itemDescription", "product")

print("\n\n=== PART B: DataFrames ===")

# f1 with DataFrame
f1_df = (
    df.select("customer_id", "product")
      .distinct()
      .groupBy("customer_id")
      .count()
      .orderBy(F.col("count").desc())
      .limit(10)
)
print("\n=== f1 DataFrame: Top 10 customers ===")
f1_df.show()
f1_df.write.mode("overwrite").csv("output/f1_df")

# f2 with DataFrame
f2_df = (
    df.select("customer_id", "date", "month")
      .distinct()
      .groupBy("month")
      .count()
      .orderBy("month")
)
print("\n=== f2 DataFrame: Baskets per month ===")
f2_df.show()
f2_df.write.mode("overwrite").csv("output/f2_df")

# f3 with DataFrame - most frequent pair per month
# This is trickier in DataFrames; collect_list then explode pairs
from pyspark.sql.functions import collect_list, explode, struct

baskets_df = (
    df.groupBy("customer_id", "date", "month")
      .agg(collect_list("product").alias("products"))
)

# UDF to generate pairs
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf

def pairs_udf_func(items):
    items = sorted(set(items))
    return [f"{items[i]}|{items[j]}" for i in range(len(items)) for j in range(i+1, len(items))]

pairs_udf = udf(pairs_udf_func, ArrayType(StringType()))

f3_df = (
    baskets_df
    .withColumn("pair", explode(pairs_udf("products")))
    .groupBy("month", "pair")
    .count()
    .orderBy("month", F.col("count").desc())
)
print("\n=== f3 DataFrame: Frequent pairs per month ===")
f3_df.show()
f3_df.write.mode("overwrite").csv("output/f3_df")

# f4 with DataFrame
f4_df = (
    df.groupBy("customer_id", "date")
      .count()
      .groupBy("customer_id")
      .agg(F.stddev("count").alias("stddev_items"))
      .orderBy(F.col("stddev_items").desc())
      .limit(1)
)
print("\n=== f4 DataFrame: Most irregular customer ===")
f4_df.show()
f4_df.write.mode("overwrite").csv("output/f4_df")

sc.stop()
