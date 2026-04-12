"""
EXAMPLE: Association Rule Generation using PySpark DataFrames

This is analogous to your Task 4 which reads baskets and frequent pairs
then generates association rules with confidence and interest filtering.

The AssociationRuleGenerator class mirrors exactly what your task requires:
  - Constructor: baskets_df, frequent_pairs_df, confidence c, interest i
  - generateRules()
  - computeConfidence()
  - computeInterest()
  - transform()

Run with:
  spark-submit task4_example.py
"""

from pyspark.sql.functions import split, col
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import os

spark = SparkSession.builder \
    .appName("AssociationRules") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# AssociationRuleGenerator CLASS
class AssociationRuleGenerator:

    def __init__(self, baskets_df, frequent_pairs_df, confidence_threshold, interest_threshold):
        """
        baskets_df:         DataFrame with columns (basket_id, item)
                            one row per item per basket
        frequent_pairs_df:  DataFrame with columns (itemA, itemB, support)
                            output from PCY Pass 2
        confidence_threshold (c): minimum confidence to keep a rule
        interest_threshold (i):   minimum interest to keep a rule
        """
        self.baskets_df = baskets_df
        self.frequent_pairs_df = frequent_pairs_df
        self.c = confidence_threshold
        self.i = interest_threshold

        # Pre-compute total number of baskets (used for interest calculation)
        self.total_baskets = baskets_df.select("basket_id").distinct().count()

        # Pre-compute support of each individual item
        # support(X) = number of baskets containing X
        self.item_support_df = (
            baskets_df
            .groupBy("item")
            .agg(F.countDistinct("basket_id").alias("support"))
        )

    # generateRules()
    # From each frequent pair (A, B), generate two candidate rules:
    #   A -> B  and  B -> A
    # Remove duplicates and trivial rules
    def generateRules(self):
        pairs = self.frequent_pairs_df

        # Rule direction 1: A -> B
        rules_ab = pairs.select(
            F.col("itemA").alias("antecedent"),
            F.col("itemB").alias("consequent"),
            F.col("support").alias("pair_support")
        )

        # Rule direction 2: B -> A
        rules_ba = pairs.select(
            F.col("itemB").alias("antecedent"),
            F.col("itemA").alias("consequent"),
            F.col("support").alias("pair_support")
        )

        # Combine both directions
        all_rules = rules_ab.union(rules_ba)

        # Remove trivial rules where antecedent == consequent (shouldn't happen but safety check)
        all_rules = all_rules.filter(F.col("antecedent") != F.col("consequent"))

        # Remove duplicates
        self.rules_df = all_rules.distinct()
        return self.rules_df

    # computeConfidence()
    # confidence(A -> B) = support(A, B) / support(A)
    # Filter rules where confidence >= threshold c
    def computeConfidence(self):
        # Join rules with item support to get support(A)
        rules_with_conf = (
            self.rules_df
            .join(
                self.item_support_df.withColumnRenamed("item", "antecedent")
                                    .withColumnRenamed("support", "antecedent_support"),
                on="antecedent"
            )
            .withColumn(
                "confidence",
                F.col("pair_support") / F.col("antecedent_support")
            )
            .filter(F.col("confidence") >= self.c)
        )

        self.rules_df = rules_with_conf
        return self.rules_df

    # computeInterest()
    # interest(A -> B) = confidence(A -> B) - P(B)
    # P(B) = support(B) / total_baskets
    # Filter rules where |interest| >= threshold i
    def computeInterest(self):
        total = self.total_baskets

        rules_with_interest = (
            self.rules_df
            .join(
                self.item_support_df.withColumnRenamed("item", "consequent")
                                    .withColumnRenamed("support", "consequent_support"),
                on="consequent"
            )
            .withColumn("prob_consequent", F.col("consequent_support") / total)
            .withColumn("interest", F.col("confidence") - F.col("prob_consequent"))
            .filter(F.abs(F.col("interest")) >= self.i)
        )

        self.rules_df = rules_with_interest
        return self.rules_df

    # transform()
    # Given a set of items a user has, look up matching rules and return
    # recommended items (right-hand side of matching rules)
    def transform(self, input_items):
        """
        input_items: list of items e.g. ["bread", "butter"]
        returns: list of recommended items
        """
        # Create a small DataFrame from the input items
        input_df = spark.createDataFrame(
            [(item,) for item in input_items],
            ["antecedent"]
        )

        # Find all rules where antecedent is in the input set
        recommendations = (
            self.rules_df
            .join(input_df, on="antecedent")
            .select("consequent", "confidence", "interest")
            .filter(~F.col("consequent").isin(input_items))  # don't recommend items already in basket
            .distinct()
            .orderBy(F.col("confidence").desc())
        )

        return recommendations


# EXAMPLE USAGE

# Read data
baskets_df = spark.read.csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "baskets.csv"), header=True, inferSchema=True)

baskets_df = baskets_df.withColumn("basket_id", F.concat_ws("_", F.col("Member_number"), F.col("Date"))) \
                       .withColumnRenamed("itemDescription", "item")

# Read pair data from task 3
raw_pairs_df = spark.read.text(os.path.join(os.path.dirname(os.path.abspath(__file__)), "pair_data"))

# Ex result: arr[0] = "itemA|itemB", arr[1] = "support"
split_tab = split(col("value"), "\t")

pairs_string = split_tab.getItem(0)
support_col = split_tab.getItem(1).cast("int")

split_pipe = split(pairs_string, "\\|")
itemA_col = split_pipe.getItem(0)
itemB_col = split_pipe.getItem(1)

pairs_df = raw_pairs_df.select(
    itemA_col.alias("itemA"),
    itemB_col.alias("itemB"),
    support_col.alias("support")
)

# Create the generator with thresholds
generator = AssociationRuleGenerator(
    baskets_df=baskets_df,
    frequent_pairs_df=pairs_df,
    confidence_threshold=0.05,   # keep rules with confidence >= 50%
    interest_threshold=0.02    # keep rules with |interest| >= 0.1
)

# Run the pipeline
print("\n=== Step 1: Generate candidate rules ===")
rules = generator.generateRules()
rules.show(truncate=False)

print("\n=== Step 2: Filter by confidence ===")
rules_conf = generator.computeConfidence()
rules_conf.show(truncate=False)

print("\n=== Step 3: Filter by interest ===")
rules_final = generator.computeInterest()
rules_final.show(truncate=False)

print("\n=== Step 4: Recommend items for a user who bought ['beef'] ===")
recommendations = generator.transform(["beef"])
recommendations.show(truncate=False)

# Save final rules
rules_final.write.mode("overwrite").csv("output/association_rules")
print("\nRules saved to output/association_rules")


spark.stop()
