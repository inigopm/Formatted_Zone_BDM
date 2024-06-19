from pymongo import MongoClient

class MongoDBUtils:
    @staticmethod
    def create_collection(logger, vm_host, mongodb_port, database_name, collection_name):
        # Connect to MongoDB
        client = MongoClient(vm_host, int(mongodb_port))
        try:
            # Access the database
            db = client[database_name]

            # Create a new collection
            db.create_collection(collection_name)

            # Log collection information
            logger.info(f"Collection '{collection_name}' created in database '{database_name}'")

        except Exception as e:
            logger.error(
                f"An error occurred during the creation of the collection: {collection_name} in MongoDB database:"
                f" {database_name}. The error is: {e}")

        finally:
            # Close the client connection
            client.close()

    @staticmethod
    def drop_collection(logger, vm_host, mongodb_port, db_name, collection_name):
        # Connect to MongoDB
        client = MongoClient(vm_host, int(mongodb_port))
        try:
            # Access the specified database
            db = client[db_name]

            # Drop the collection
            db[collection_name].drop()

        except Exception as e:
            logger.error(
                f"An error occurred during the dropping of the collection: {collection_name} in MongoDB database:"
                f" {db_name}. The error is: {e}")

        finally:
            # Close the client connection
            client.close()

    @staticmethod
    def read_collection(logger, spark, vm_host, mongodb_port, db_name, collection_name):
        client = MongoClient(vm_host, int(mongodb_port))
        try:
            uri = f"mongodb://{vm_host}:{mongodb_port}/{db_name}.{collection_name}"

            df = spark.read.format("mongo") \
                .option('uri', uri) \
                .option('encoding', 'utf-8-sig') \
                .load()

            return df

        except Exception as e:
            logger.error(
                f"An error occurred while reading the collection: {collection_name} from MongoDB database: {db_name}."
                f" The error is: {e}")

        finally:
            # Close the client connection and SparkSession
            client.close()

    @staticmethod
    def write_to_collection(logger, vm_host, mongodb_port, db_name, collection_name, dataframe, append=True):
        client = MongoClient(vm_host, int(mongodb_port))
        try:
            uri = f"mongodb://{vm_host}:{mongodb_port}/{db_name}.{collection_name}"

            if not append:
                MongoDBUtils.drop_collection(logger, vm_host, mongodb_port, db_name, collection_name)

            dataframe.write.format("mongo") \
                .option("uri", uri) \
                .option("encoding", "utf-8-sig") \
                .mode("append") \
                .save()

        except Exception as e:
            logger.error(
                f"An error occurred while writing to the collection: {collection_name} in MongoDB database: {db_name}."
                f" The error is: {e}")

        finally:
            # Close the client connection
            client.close()