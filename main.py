from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("BarcelonaHousingAnalysis").getOrCreate()

# Define base path for local files
base_path = "C:/Users/inigo/Documents/UPC/Cuatri2/BDM/Formatted_Zone_BDM/P2_data"

# Load Idealista data
idealista_df = spark.read.parquet(f"{base_path}/idealista/*/*.parquet")

# Load income data
income_df = spark.read.json(f"{base_path}/income_opendata/income_opendata_neighborhood.json")

# Load lookup tables
income_lookup_district_df = spark.read.json(f"{base_path}/lookup_tables/income_lookup_district.json")
income_lookup_neighborhood_df = spark.read.json(f"{base_path}/lookup_tables/income_lookup_neighborhood.json")
rent_lookup_district_df = spark.read.json(f"{base_path}/lookup_tables/rent_lookup_district.json")
rent_lookup_neighborhood_df = spark.read.json(f"{base_path}/lookup_tables/rent_lookup_neighborhood.json")

# Remove duplicates from Idealista data
idealista_df = idealista_df.dropDuplicates(["propertyCode"])

# Reconcile neighborhood and district names in Idealista data using lookup tables
idealista_df = idealista_df.join(rent_lookup_neighborhood_df, idealista_df["neighborhood"] == rent_lookup_neighborhood_df["ne_re"], "left")
idealista_df = idealista_df.withColumn("neighborhood_reconciled", F.coalesce("ne_re", "neighborhood"))

idealista_df = idealista_df.join(rent_lookup_district_df, idealista_df["district"] == rent_lookup_district_df["di_re"], "left")
idealista_df = idealista_df.withColumn("district_reconciled", F.coalesce("di_re", "district"))

