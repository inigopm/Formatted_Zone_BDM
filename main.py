import os
import argparse
import findspark
import logging.handlers
from dotenv import load_dotenv
from pyspark.sql.types import *

from src.data_formatters.data_formatter import DataFormatter
# from src.descriptive_analysis.data_description import DataDescription
# from src.predictive_analysis.data_modeling import DataModeling

# Create logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create file handler which logs debug messages
log_file = os.path.join('logs', 'main.log')
log_dir = os.path.dirname(log_file)

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1024 * 1024, backupCount=5)
file_handler.setLevel(logging.DEBUG)

# Create console handler which logs info messages
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Initialize Spark Locally
findspark.init()

# Load environment variables from config..env
load_dotenv()

# Define VM_HOST AND VM_USER parameters from environment variables
VM_HOST = os.getenv('VM_HOST')
VM_USER = os.getenv('VM_USER')

# Define MongoDB parameters from environment variables
MONGODB_PORT = os.getenv('MONGODB_PORT')
FORMATTED_DB = os.getenv('FORMATTED_DB')
PERSISTENT_DB = os.getenv('PERSISTENT_DB')
EXPLOITATION_DB = os.getenv('EXPLOITATION_DB')


def main():
    # # Create argument parser
    # parser = argparse.ArgumentParser(description='Formatted and Exploitation Landing Zones')

    # # Add argument for execution mode
    # parser.add_argument('exec_mode', type=str, choices=['data-formatting', 'data-prediction', 'data-description'],
    #                     help='Execution mode')

    # # Add argument for action within data-formatting mode
    # parser.add_argument('action', type=str, choices=['merge-lookup-tables', 'fix-data-types', 'drop-duplicates',
    #                                                  'reconcile-data', 'train-model', 'predict'],
    #                     help='Action within data-formatting mode')



    # # Parse command line arguments
    # args = parser.parse_args()
    # exec_mode = args.exec_mode
    # action = args.action

    # Initialize a DataCollector instance
    data_formatter = DataFormatter(logger, VM_HOST, MONGODB_PORT, PERSISTENT_DB, FORMATTED_DB)


    logger.info('Merging and deduplicate lookup tables...')

    data_formatter.merge_district_lookup_table("lookup_table_district",
                                                "income_lookup_district", "rent_lookup_district")
    data_formatter.merge_neighborhood_lookup_table("lookup_table_neighborhood",
                                                    "income_lookup_neighborhood", "rent_lookup_neighborhood")

    logger.info('Lookup table merge and deduplication completed.')
        


if __name__ == '__main__':
    main()