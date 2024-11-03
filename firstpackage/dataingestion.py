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

def load_data_from_db(db_path, sql_query):
    """
    Loads data from a SQLite database using the provided SQL query.

    Args:
        db_path (str): Path to the SQLite database file.
        sql_query (str): SQL query to execute.

    Returns:
        pandas.DataFrame: DataFrame containing the results of the query.
    """

    engine = sa.create_engine(db_path)
    df = pd.read_sql_query(sql_query, engine)
    return df