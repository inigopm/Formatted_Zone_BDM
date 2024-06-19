from datetime import datetime
from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import col, max as spark_max, to_date, concat, lit, explode, levenshtein, when, array_union, \
    array_distinct
from pyspark.sql.functions import lower, regexp_replace
from src.utils.mongo_utils import MongoDBUtils


class DataFormatter:

    def __init__(self, logger, vm_host, mongodb_port, persistent_db, formatted_db):
        self.logger = logger
        self.vm_host = vm_host
        self.mongodb_port = mongodb_port
        self.persistent_db = persistent_db
        self.formatted_db = formatted_db

        try:
            # Create a SparkSession
            self.spark = SparkSession.builder \
                .master("local[*]") \
                .appName("Unify Lookup District") \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()
        except Exception as e:
            self.logger.error(
                f"An error occurred during the creation of the Spark Configuration during the creation of DataFormatter class."
                f" This is the error: {e}")

    # def create_collection(self, database_name, collection_name):
    #     # Connect to MongoDB
    #     client = MongoClient(self.vm_host, int(self.mongodb_port))
    #     try:
    #         # Access the database
    #         db = client[database_name]
    #
    #         # Create a new collection
    #         db.create_collection(collection_name)
    #
    #         # Log collection information
    #         self.logger.info(f"Collection '{collection_name}' created in database '{database_name}'")
    #
    #     except Exception as e:
    #         self.logger.error(
    #             f"An error occurred during the creation of the collection: {collection_name} in MongoDB database:"
    #             f" {database_name}. The error is: {e}")
    #
    #     finally:
    #         # Close the client connection
    #         client.close()
    #
    # def drop_mongo_collection(self, db_name, collection_name):
    #     # Connect to MongoDB
    #     client = MongoClient(self.vm_host, int(self.mongodb_port))
    #     try:
    #
    #         # Access the specified database
    #         db = client[db_name]
    #
    #         # Drop the collection
    #         db[collection_name].drop()
    #
    #         # Close the MongoDB client
    #         client.close()
    #
    #     except Exception as e:
    #         self.logger.error(
    #             f"An error occurred during the dropping of the collection: {collection_name} in MongoDB database:"
    #             f" {db_name}. The error is: {e}")
    #
    #     finally:
    #         # Close the client connection
    #         client.close()
    #
    # def read_mongo_collection(self, db_name, collection_name):
    #     uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{db_name}.{collection_name}"
    #     df = self.spark.read.format("mongo") \
    #         .option('uri', uri) \
    #         .option('encoding', 'utf-8-sig') \
    #         .load()
    #     return df
    #
    # def write_to_mongo_collection(self, db_name, collection_name, dataframe, append = True):
    #     uri = f"mongodb://{self.vm_host}:{self.mongodb_port}/{db_name}.{collection_name}"
    #
    #     if not append:
    #         self.drop_mongo_collection(db_name, collection_name)
    #
    #     dataframe.write.format("mongo") \
    #         .option("uri", uri) \
    #         .option("encoding", "utf-8-sig") \
    #         .mode("append") \
    #         .save()

    def merge_dataframes(self, input_collection1, input_collection2):
        try:
            self.logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
            x1_df = MongoDBUtils.read_collection(
                self.logger, self.spark, self.vm_host, self.mongodb_port,
                self.persistent_db, input_collection1)

            self.logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
            x2_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                 self.persistent_db, input_collection2)

            self.logger.info("Merging dataframes...")
            unified_df = x1_df.union(x2_df)

            self.logger.info("Dataframes merged successfully.")
            return unified_df

        except Exception as e:
            self.logger.error(f"An error occurred while merging dataframes: {e}")

    def drop_duplicates(self, dataframe):
        deduplicated_df = dataframe.dropDuplicates()
        self.logger.info("Deduplication completed successfully.")
        return deduplicated_df

    def filter_existing_records(self, result_df, existing_df):
        self.logger.info("Filtering out already existing records...")
        if not existing_df.isEmpty:
            existing_ids = existing_df.select("_id").collect()
            result_df = result_df.filter(~result_df["_id"].isin([row["_id"] for row in existing_ids]))
        return result_df

    def merge_district_lookup_table(self, output_collection, input_collection1, input_collection2):
        try:
            self.logger.info(f"Formatting Lookup-table {output_collection} and moving it to Formatted Zone.")

            MongoDBUtils.create_collection(self.logger, self.vm_host, self.mongodb_port,
                                           self.formatted_db, output_collection)

            # income_lookup_district
            self.logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
            x1_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                 self.persistent_db, input_collection1)

            # rent_lookup_district
            self.logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
            x2_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                 self.persistent_db, input_collection2)

            # Perform join operation on _id column
            joined_df = x1_df.join(x2_df, x1_df["_id"] == x2_df["_id"], "inner")

            # Select and rename the required columns
            result_df = joined_df.select(
                x1_df["_id"],
                x2_df["di"].alias("district"),
                x2_df["di_n"].alias("district_name"),
                x2_df["di_re"].alias("district_reconciled"),
                array_distinct(array_union(x1_df["neighborhood_id"], x2_df["ne_id"])).alias("neighborhood_id")
            )

            self.logger.info(f"Saving new records into '{output_collection}' collection in MongoDB...")
            MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                             output_collection, result_df)

            self.logger.info(f'Formatting {output_collection} Lookup Table completed successfully.')

        except Exception as e:
            self.logger.exception(e)

    def merge_neighborhood_lookup_table(self, output_collection, input_collection1, input_collection2):
        try:
            self.logger.info(f"Formatting Lookup-table {output_collection} and moving it to Formatted Zone.")

            MongoDBUtils.create_collection(self.logger, self.vm_host, self.mongodb_port,
                                           self.formatted_db, output_collection)

            # income_lookup_neighborhood
            self.logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
            x1_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                 self.persistent_db, input_collection1)

            # rent_lookup_neighborhood
            self.logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
            x2_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                 self.persistent_db, input_collection2)

            # Perform join operation on _id column
            joined_df = x1_df.join(x2_df, x1_df["_id"] == x2_df["_id"], "left")

            # Select the columns from joined_df and drop the second _id column
            joined_df = joined_df.select(x1_df["_id"], x1_df["neighborhood"], x1_df["neighborhood_name"],
                                         x1_df["neighborhood_reconciled"],
                                         x2_df["ne"], x2_df["ne_n"], x2_df["ne_re"])

            self.logger.info(f"Saving new records into '{output_collection}' collection in MongoDB...")
            MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                             output_collection, joined_df)

            self.logger.info(f'Formatting {output_collection} Lookup Table completed successfully.')

        except Exception as e:
            self.logger.exception(e)

    def transform_idealista_to_latest_info(self, db_name, collection_name):

        try:
            # Load the MongoDB collection into a PySpark DataFrame
            df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                              db_name, collection_name)

            # Explode the 'value' array
            exploded_df = df.withColumn("exploded_value", explode(col("value"))).drop("value")

            # Transform the collection
            transformed_df = exploded_df.selectExpr("_id as date", "exploded_value.*") \
                .withColumn("new_id", concat(col("date"), lit("_"), col("propertyCode"))) \
                .withColumnRenamed("new_id", "_id")

            transformed_df = transformed_df.withColumn("date", to_date(col("date"), "yyyy_MM_dd"))

            latest_dates = transformed_df.groupBy("propertyCode").agg(spark_max("date").alias("latestDate"))

            # Perform a left semi join to filter the transformed_df based on the latest dates
            filtered_df = transformed_df.join(latest_dates,
                                              (transformed_df.propertyCode == latest_dates.propertyCode) & (
                                                      transformed_df.date == latest_dates.latestDate), "left_semi")
            # Drop the _id column
            filtered_df = filtered_df.drop("_id")

            # Rename the propertyCode column to _id
            filtered_df = filtered_df.withColumnRenamed("propertyCode", "_id")

            # Reorder the columns to make _id the first column
            filtered_df = filtered_df.select("_id", *filtered_df.columns[:-1])

            self.logger.info("Transformation of idealista completed successfully.")

            return filtered_df

        except Exception as e:
            self.logger.error("Error occurred during transformation: %s", str(e))

    def convert_collection_data_types(self, db_name_input, db_name_output, collection_name, new_schema):
        # Read the collection from MongoDB
        df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                          db_name_input, collection_name)

        # Apply the new schema to the DataFrame
        df_with_new_schema = df
        for field in new_schema.fields:
            if field.name in df.columns:
                df_with_new_schema = df_with_new_schema.withColumn(field.name, df[field.name].cast(field.dataType))

        # Write the DataFrame back to the same collection
        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, db_name_output, collection_name,
                                         df_with_new_schema, True)

    def reconcile_data_with_lookup(self, input_dF, lookup_df, input_join_attribute, lookup_join_attribute, lookup_id,
                                   input_id_reconcile, threshold):
        try:

            input_dF = input_dF.withColumn(input_join_attribute, lower(input_dF[input_join_attribute]))
            input_dF = input_dF.withColumn(input_join_attribute, regexp_replace(input_dF[input_join_attribute],
                                                                                "[\u0300-\u036F]", ""))
            # input_dF = input_dF.withColumn(input_join_attribute, trim(input_dF[input_join_attribute]))

            self.logger.info("Performing join and reconciliation...")
            resultDF = input_dF.join(lookup_df,
                                     when(
                                         levenshtein(col(input_join_attribute),
                                                     col(lookup_join_attribute)) <= threshold,
                                         True).otherwise(False),
                                     "left"
                                     ).withColumn(input_id_reconcile, lookup_df[lookup_id]) \
                .drop(lookup_df[lookup_join_attribute]) \
                .drop(lookup_df[lookup_id])

            #resultDF.show()
            return resultDF
        except Exception as e:
            self.logger.error(f"An error occurred during data reconciliation: {e}")

    def drop_duplicates_action(self):
        self.logger.info('Read collection "income" from MongoDB.')
        income_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                 self.formatted_db, "income")

        self.logger.info('Duplicates dropped for collection "income".')
        deduplicated_income_df = self.drop_duplicates(income_df)

        self.logger.info('Read collection "building_age" from MongoDB.')
        building_age_df = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host,
                                                       self.mongodb_port, self.formatted_db, "building_age")

        self.logger.info('Duplicates dropped for collection "building_age".')
        deduplicated_building_age_df = self.drop_duplicates(building_age_df)

        self.logger.info('Writing deduplicated data back to MongoDB.')

        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                         "income", deduplicated_income_df)
        self.logger.info('Deduplicated "income" data written to MongoDB.')

        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                         "building_age", deduplicated_building_age_df)
        self.logger.info('Deduplicated "building_age" data written to MongoDB.')

        self.logger.info('Reading collection "idealista" from MongoDB.')

        df = self.transform_idealista_to_latest_info(self.formatted_db, "idealista")
        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                         "idealista_cleaned", df)
        self.logger.info('Deduplicated "idealista" data written to MongoDB.')

    def reconcile_data_action(self):
        self.logger.info(f"Reading lookup data from MongoDB...")
        lookupDF_district = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host,
                                                         self.mongodb_port, self.formatted_db,
                                                         "lookup_table_district")
        lookupDF_district = lookupDF_district.select("district_name", "_id")
        lookupDF_district = lookupDF_district.withColumnRenamed("district_name", "district")
        lookupDF_district.cache()

        lookupDF_neighborhood = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host,
                                                             self.mongodb_port, self.formatted_db,
                                                             "lookup_table_neighborhood")
        lookupDF_neighborhood = lookupDF_neighborhood.select("neighborhood_name", "_id")
        lookupDF_neighborhood = lookupDF_neighborhood.withColumnRenamed("neighborhood_name", "neighborhood")
        lookupDF_neighborhood.cache()

        inputDF_income = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                      self.persistent_db, "income")

        inputDF_building_age = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host,
                                                            self.mongodb_port, self.persistent_db, "building_age")

        input_idealista = MongoDBUtils.read_collection(self.logger, self.spark, self.vm_host, self.mongodb_port,
                                                       self.formatted_db, "idealista_cleaned")

        input_idealista = input_idealista.withColumnRenamed("district", "district_name")
        input_idealista = input_idealista.withColumnRenamed("neighborhood", "neighborhood_name")
        input_idealista = input_idealista.withColumn('district_id', lit(''))
        input_idealista = input_idealista.withColumn('neighborhood_id', lit(''))

        income_rec = self.reconcile_data_with_lookup(inputDF_income, lookupDF_district,
                                        "district_name", "district", "_id", "district_id", 2)

        final_inc_rec = self.reconcile_data_with_lookup(income_rec, lookupDF_neighborhood,
                                        "neigh_name ", "neighborhood", "_id", "_id", 3)

        building_age_rec = self.reconcile_data_with_lookup(inputDF_building_age, lookupDF_district,
                                        "district_name", "district", "_id", "district_id", 2)

        final_build_age_rec = self.reconcile_data_with_lookup(building_age_rec, lookupDF_neighborhood,
                                                                 "neigh_name", "neighborhood", "_id", "_id",
                                                                 3)

        idealista_rec = self.reconcile_data_with_lookup(input_idealista, lookupDF_district,
        "district_name", "district", "_id",
        "district_id", 2)

        final_idealista_rec = self.reconcile_data_with_lookup(idealista_rec, lookupDF_neighborhood,
        "neighborhood_name", "neighborhood", "_id",
        "neighborhood_id", 2)

        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                                 "income_reconciled", final_inc_rec)
        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                                 "building_age_reconciled", final_build_age_rec)
        MongoDBUtils.write_to_collection(self.logger, self.vm_host, self.mongodb_port, self.formatted_db,
                                                 "idealista_reconciled", final_idealista_rec)