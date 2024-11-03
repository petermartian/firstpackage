# Install necessary packages
!pip install gdown googledrivedownloader

import pandas as pd
from sqlalchemy import create_engine, text
import gdown
from google_drive_downloader import GoogleDriveDownloader as gdd
import os
import logging

# Set up logging
logger = logging.getLogger('data_ingestion')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Download the database file from Google Drive using gdown
file_id = '1DBaIAdS8gG3F9zZp_1PnMXY0QpmohcUU'
output_file = 'Maji_Ndogo_farm_survey_small.db'
gdown.download(id=file_id, output=output_file, quiet=False)

# Alternatively, download the database file from Google Drive using GoogleDriveDownloader
download_path = os.path.join(os.getcwd(), output_file)
gdd.download_file_from_google_drive(file_id=file_id, dest_path=download_path, unzip=False)

# Create a SQLAlchemy engine to connect to the database
engine = create_engine(f'sqlite:///{download_path}')

# Your SQL query
sql_query = """
SELECT *
FROM geographic_features AS gf
JOIN weather_features AS wf ON gf.Field_ID = wf.Field_ID
JOIN soil_and_crop_features AS scf ON gf.Field_ID = scf.Field_ID
JOIN farm_management_features AS fm ON gf.Field_ID = fm.Field_ID
"""

# Use Pandas to execute the query and store the result in a DataFrame
with engine.connect() as connection:
    MD_agric_df = pd.read_sql_query(text(sql_query), connection)

# Now you have the data in the MD_agric_df DataFrame
# You can perform further operations on it as needed

# Define functions for data ingestion
def create_db_engine(db_path):
    try:
        engine = create_engine(db_path)
        with engine.connect() as conn:
            pass
        logger.info("Database engine created successfully.")
        return engine
    except ImportError:
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise
    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise

def query_data(engine, sql_query):
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e:
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e

def read_from_web_CSV(URL):
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e

# Example usage of the functions
db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'
engine = create_db_engine(db_path)
df = query_data(engine, sql_query)
print(df.head())

weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
weather_df = read_from_web_CSV(weather_data_URL)
print(weather_df.head())
