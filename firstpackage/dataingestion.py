!pip install gdown

import pandas as pd
from sqlalchemy import create_engine, text
import gdown

# Download the database file from Google Drive
file_id = '1DBaIAdS8gG3F9zZp_1PnMXY0QpmohcUU'
output_file = 'Maji_Ndogo_farm_survey_small.db'
gdown.download(id=file_id, output=output_file, quiet=False)

# Create an engine for the database (using the downloaded file)
engine = create_engine(f'sqlite:///{output_file}')

!pip install googledrivedownloader
from google_drive_downloader import GoogleDriveDownloader as gdd
import pandas as pd
from sqlalchemy import create_engine, text
import os

# Download the database file from Google Drive
file_id = '1DBaIAdS8gG3F9zZp_1PnMXY0QpmohcUU'
output_file = 'Maji_Ndogo_farm_survey_small.db'

# Specify the full download path including the filename
# This ensures the file is downloaded to a specific location
download_path = os.path.join(os.getcwd(), output_file)

gdd.download_file_from_google_drive(file_id=file_id,
                                    dest_path=download_path, # Use the full download path
                                    unzip=False)

# Create a SQLAlchemy engine to connect to the database
engine = create_engine(f'sqlite:///{download_path}') # Use the full download path here as well

# Your SQL query
sql_query = """SELECT *
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


import pandas as pd # importing the Pandas package with an alias, pd
from sqlalchemy import create_engine, text # Importing the SQL interface. If this fails, run !pip install sqlalchemy in another cell.
import matplotlib.pyplot as plt
import seaborn as sns


# Create an engine for the database
engine = create_engine('sqlite:///Maji_Ndogo_farm_survey_small.db') #Make sure to have the .db file in the same directory as this notebook, and the file name matches.


sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""

# Create a connection object
with engine.connect() as connection:

    # Use Pandas to execute the query and store the result in a DataFrame
    MD_agric_df = pd.read_sql_query(text(sql_query), connection)


weather_station_df = pd.read_csv("https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv")
weather_station_mapping_df = pd.read_csv("https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv")

def create_db_engine(db_path):
    engine = create_engine(db_path)
    return engine

def query_data(engine, sql_query):
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_query), connection)
        return df

create_db_engine('sqlite:///Maji_Ndogo_farm_survey_small.db')

SQL_engine = create_db_engine('sqlite:///Maji_Ndogo_farm_survey_small.db')

sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""


df = query_data(SQL_engine, sql_query)
df

sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""


df = query_data(create_db_engine('sqlite:///Maji_Ndogo_farm_survey_small.db'), sql_query)
df



from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'

sql_query = """
SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""

weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
weather_mapping_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"

def create_db_engine(db_path):
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e

def query_data(engine, sql_query):
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
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


# Testing module functions
field_df = query_data(create_db_engine(db_path), sql_query)
weather_df = read_from_web_CSV(weather_data_URL)
weather_mapping_df = read_from_web_CSV(weather_mapping_data_URL)




