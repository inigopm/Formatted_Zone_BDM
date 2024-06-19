import os
from pyspark.sql import SparkSession

from pyspark.sql.types import StringType
import os
import unicodedata
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType

class LoadtoFormatted:

    def __init__(self, spark):
        # Load data to PySpark

        print('Loading income data to PySpark...')
        df_income = self.loadToSpark(spark, income_path)
        if df_income is not None:
            print('Income data loaded!')

        print('Loading lookup tables data to PySpark...')
        df_lookup = self.loadToSpark(spark, lookuptables_path)
        if df_lookup is not None:
            print('Lookup tables data loaded!')

        print('Loading idealista data to PySpark...')
        dfs_idealista = {}
        folder_names = os.listdir(idealista_path)
        for folder in folder_names:
            file_path = os.path.join(idealista_path, folder)
            if os.path.isdir(file_path):
                df = next(iter(self.loadToSpark(spark, file_path).values()))
                if df is not None:
                    dfs_idealista[folder] = df
        if dfs_idealista:
            print('Idealista data loaded!')

        self.dfs = {'income': df_income, 'lookup': df_lookup, 'idealista': dfs_idealista}
        
        self.spark = spark

        return None

    def loadToSpark(self, spark, path):

        file_names = os.listdir(path)

        dfs = {}
        for file_name in file_names:
            file_path = os.path.join(path, file_name)
            if file_name.split('.')[-1] == 'json':
                df = spark.read.option('header', True).json(file_path)
            elif file_name.split('.')[-1] == 'csv':
                df = spark.read.option('header', True).csv(file_path)
            elif file_name.split('.')[-1] == 'parquet':
                df = spark.read.option('header', True).parquet(file_path)
            else:
                df = None
                # print("Uningestible file format: ", file_name)

            if df is not None:
                dfs[file_name] = df

        return dfs
    
def explode_column(dfs, column_name):
    output_dfs = []
    for df in dfs:
        if column_name in df.columns:
            for subcol in df.select(column_name + '.*').columns:
                df = df.withColumn(subcol, col(column_name + '.' + subcol))
            output_dfs.append(df.drop(column_name))
    return output_dfs

def get_all_columns(dfs):
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    return list(all_columns) if all_columns else []

def ensure_all_columns(df, all_columns):
    # Add missing columns with null values
    for col_name in all_columns:
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(None))

    # Reorder columns to match the order in all_columns
    ordered_columns = [col_name for col_name in all_columns if col_name in df.columns]
    df = df.select(*ordered_columns)

    return df

def join_and_union(dfs, df_lookup, join_column, lookup_column, ensure_same_schema=False):
    joined_list = []
    all_columns = get_all_columns(dfs)
    all_columns += get_all_columns([df_lookup])

    for df in dfs:
        if ensure_same_schema:
            df = ensure_all_columns(df, all_columns)

        # Ensure compatible column types
        '''for col_name in df.columns:
            if col_name in df_lookup.columns:
                lookup_data_type = df_lookup.schema[col_name].dataType
                if df.schema[col_name].dataType != lookup_data_type:
                    # Handle mismatched column types
                    if isinstance(lookup_data_type, StructType):
                        # If the lookup column type is a struct, cast the DataFrame column to match
                        df = df.withColumn(col_name, df[col_name].cast(lookup_data_type))
                    else:
                        # Otherwise, add or replace the column with null values
                        df = df.withColumn(col_name, lit(None).cast(lookup_data_type))'''
        # Ensure compatible column types on join columns
        lookup_data_type = df_lookup.schema[lookup_column].dataType
        if df.schema[join_column].dataType != lookup_data_type:
            # Handle mismatched column types
            if isinstance(lookup_data_type, StructType):
                # If the lookup column type is a struct, cast the DataFrame column to match
                df = df.withColumn(
                    join_column, df[join_column].cast(lookup_data_type))
            else:
                # Otherwise, add or replace the column with null values
                df = df.withColumn(join_column, lit(None).cast(lookup_data_type))

        # print_shape_info(df, f"{df_name} before join")
        joined_df = df.join(df_lookup, df[join_column] == df_lookup[lookup_column], 'left')
        # print_shape_info(joined_df, f"{df_name} after join")
        
        # Alias columns to avoid ambiguous references
        rdd = joined_df.rdd
        # We use RDDs because withColumnRenamed
        # changes the name of all columns with the old name, so if we
        # have two columns named equally, after renaming them,
        # they will keep having the same name

        aliased_columns = []
        for col_name in joined_df.columns:
            aliased_name = col_name
            count = 1
            while aliased_name in aliased_columns:
                aliased_name = f"{col_name}_{count}"
                count += 1
            aliased_columns.append(aliased_name)
        
        schema = joined_df.schema
        for idx, aliased_column in enumerate(aliased_columns):
            schema[idx].name = aliased_column
        joined_df = rdd.toDF(schema)
        #joined_df = rdd.toDF(Row(*aliased_columns))
        
        # Reorder columns
        joined_df = ensure_all_columns(joined_df, all_columns)

        joined_list.append(joined_df)

    # Union
    if joined_list:
        merged_df = joined_list[0]
        for df in joined_list[1:]:
            merged_df = merged_df.union(df)
        return merged_df
    return None

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    return only_ascii.decode()

def lowercase_lookup(s):
    # Replace this with your actual string function
    return remove_accents(s.lower().replace('-', ' '))

def reconciliate_data(dfs):
        print('Starting reconciliate process...')
        merged_dfs = {}
        # Process df_income
        df_income_list = list(dfs.get('income').values())
        if df_income_list is not None and isinstance(df_income_list, list):
            # Drop not useful columns
            df_income_list = [df.drop('_id') for df in df_income_list]

            # Reconcile district
            df_lookup = dfs['lookup']['income_lookup_district.json'].select('district', 'district_reconciled')
            df_income_merged = join_and_union(df_income_list, df_lookup, 'district_name', 'district')
            # Keep only reconciled column
            df_income_merged = df_income_merged.drop('district', 'district_name', 'district_id').withColumnRenamed('district_reconciled', 'district')

            # Reconcile neigborhood
            df_lookup = dfs['lookup']['income_lookup_neighborhood.json'].select(
                'neighborhood', 'neighborhood_reconciled')
            df_income_merged = join_and_union([df_income_merged], df_lookup, 'neigh_name ', 'neighborhood')
            df_income_merged = df_income_merged.drop('neigh_name ', 'neighborhood').withColumnRenamed('neighborhood_reconciled', 'neighborhood')

            # Explode info column into year, pop, RFD
            df_income_merged = df_income_merged.withColumn('RFD', 
                col('info')[0]['RFD']).withColumn('pop', col('info')[0]['pop']).withColumn('year', col('info')[0]['year']).drop('info')

            print(f"Income after merge columns: {df_income_merged.columns}")
            merged_dfs['income'] = df_income_merged

        # Process df_idealista
        df_idealista_list = list(dfs.get('idealista').values())
        if df_idealista_list is not None and isinstance(df_idealista_list, list):
            # Drop not useful columns
            columns_to_drop = ['country', 'municipality', 'province', 'url', 'thumbnail'] #operation
            df_idealista_list = [df.drop(*columns_to_drop) for df in df_idealista_list]

            # Explode columns parkingSpace, detailedType, suggestedTexts
            df_idealista_list = explode_column(df_idealista_list, 'parkingSpace')
            df_idealista_list = explode_column(df_idealista_list, 'detailedType')
            df_idealista_list = explode_column(df_idealista_list, 'suggestedTexts')

            # Reconcile district
            df_lookup = dfs['lookup']['rent_lookup_district.json'].select('di', 'di_re')
            df_idealista_merged = join_and_union(df_idealista_list, df_lookup, 'district', 'di', ensure_same_schema=True)
            df_idealista_merged = df_idealista_merged.drop('di', 'district').withColumnRenamed('di_re', 'district')

            # Reconcile neighborhood
            df_lookup = dfs['lookup']['rent_lookup_neighborhood.json'].select('ne', 'ne_re')
            df_idealista_merged = join_and_union([df_idealista_merged], df_lookup, 'neighborhood', 'ne')
            df_idealista_merged = df_idealista_merged.drop('neighborhood', 'ne').withColumnRenamed('ne_re', 'neighborhood')

            print(f"Idealista after merge columns: {df_idealista_merged.columns}")
            merged_dfs['idealista'] = df_idealista_merged
            #==================================================================
            # TESTING
            #==================================================================
            merged_dfs['idealista'].toPandas().to_csv('idealista.csv', index=False)

        return merged_dfs

dir_path = os.getcwd()
data_path = os.path.join(dir_path, 'P2_data')
out_path = os.path.join(data_path, 'output')
idealista_path = os.path.join(data_path, 'idealista')
income_path = os.path.join(data_path, 'income_opendata')
lookuptables_path = os.path.join(data_path, 'lookup_tables')
# airquality_path = os.path.join(data_path, 'airquality_data')

python_executable_path = r'C:\Users\usuario\AppData\Local\Programs\Python\Python39\python.exe'

spark = SparkSession.builder \
                .master("local[*]") \
                .appName("Unify Lookup District") \
                .config("spark.pyspark.python", python_executable_path) \
                .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
                .getOrCreate()

formattedLoader = LoadtoFormatted(spark)
dfs = formattedLoader.dfs
merged_dfs = reconciliate_data(dfs)