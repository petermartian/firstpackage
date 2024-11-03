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
