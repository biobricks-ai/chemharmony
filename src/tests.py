import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def test_property_count_consistency():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PropertyCountConsistencyTest") \
        .config("spark.driver.memory", "32g") \
        .config("spark.executor.memory", "32g") \
        .getOrCreate()

    # Read the properties and activities tables
    properties_df = spark.read.parquet("brick/properties.parquet")
    activities_df = spark.read.parquet("brick/activities.parquet")

    # Count properties in properties table by source
    properties_count = properties_df.groupBy("source") \
        .agg(F.count("pid").alias("properties_count")) \
        .orderBy("source")

    # let's inspect th properties for BACE
    properties_df.filter(F.col("source") == "BACE").show(truncate=False)

    # Count unique properties in activities table by source
    activities_property_count = activities_df.select("pid", "source") \
        .distinct() \
        .groupBy("source") \
        .agg(F.count("pid").alias("activities_property_count")) \
        .orderBy("source")

    # Join the counts and compare
    comparison = properties_count.join(activities_property_count, "source", "outer") \
        .select(
            "source",
            F.coalesce("properties_count", F.lit(0)).alias("properties_count"),
            F.coalesce("activities_property_count", F.lit(0)).alias("activities_property_count")
        )

    # Convert to pandas for easier assertion and display
    results = comparison.toPandas()
    
    # Print the comparison results
    print("\nProperty Count Comparison by Source:")
    print("=====================================")
    print(results.to_string(index=False))
    print("\n")

    # Check for any mismatches
    mismatches = results[results['properties_count'] != results['activities_property_count']]
    
    if not mismatches.empty:
        print("Mismatches Found:")
        print("================")
        print(mismatches.to_string(index=False))
        raise AssertionError("Property count mismatch found between properties and activities tables")
    else:
        print("✓ All property counts match between properties and activities tables")

if __name__ == "__main__":
    test_property_count_consistency()
