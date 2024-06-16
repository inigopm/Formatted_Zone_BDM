def loadMongoDataFrame(collection: str, spark):
    '''
    Download data from MongoDB and store it in DataFrame format
    '''

    dataDF = spark.read.format("mongo") \
        .option('uri', f"mongodb://10.4.41.48/opendata.{collection}") \
        .load() \
        .cache()

    return dataDF

def merge_dataframes(self, df1, df2):
        return df1.union(df2)

def drop_duplicates(self, dataframe):
        return dataframe.dropDuplicates()
