import pandas as pd

from sqlalchemy import create_engine

import requests


def get_job_description(project_m1_data):

    # To extract all unique job codes from our database and find them through the API

    unique_job_codes_in_db = project_m1_data['normalized_job_code'].unique()
    unique_job_code_list = list(unique_job_codes_in_db[1:])

    # To create dictionary and build DataFrame of job codes and job descriptions

    job_code_dict = {}

    for job_code in unique_job_code_list:
        job_code_response = requests.get(f'http://api.dataatwork.org/v1/jobs/{job_code}').json()
        job_code_dict[job_code] = job_code_response['title']

    job_code_df = pd.DataFrame.from_dict(job_code_dict, orient='index', columns=['job_description'])
    job_code_df = job_code_df.reset_index()
    job_code_df = job_code_df.rename(columns={"index": "job_code"})

    # To export DataFrame with "normalized_job_codes" found through API and their respective "job_description".
    job_code_df.to_csv(f'/Users/manuelaquino/Bootcamp/project_m1/data/processed/project_m1_job_description_extracted_from_api_dataframe.csv', index=True)

    return job_code_df


def acquire(database_file_path: str):

    # This part to create connection engine

    sqlitedb_abs_path = '/Users/manuelaquino/Bootcamp/project_m1/data/raw/raw_data_project_m1.db'

    connection_string = f'sqlite:///{sqlitedb_abs_path}'

    engine = create_engine(connection_string)

    # This part is to get the full raw data from database
    print('Loading info from the following database tables...')
    print(list(pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", engine)['name'].unique()))
    project_m1_full_raw_data = pd.read_sql_query(
        """
        SELECT * 
        FROM personal_info
        JOIN career_info
        ON personal_info.uuid = career_info.uuid
        JOIN country_info
        ON country_info.uuid = career_info.uuid
        JOIN poll_info
        ON poll_info.uuid = career_info.uuid""", engine)

    # To keep only unique columns for my first raw DataFrame

    full_raw_data_unique_columns_list = list((project_m1_full_raw_data.columns.unique()))
    project_m1_data = project_m1_full_raw_data[full_raw_data_unique_columns_list]
    project_m1_data = project_m1_data.loc[:, ~project_m1_data.columns.duplicated()]

    # To export the first consolidated raw DataFrame to a processed data folder.
    project_m1_data.to_csv(f'/Users/manuelaquino/Bootcamp/project_m1/data/processed/project_m1_extracted_dataframe_data.csv', index=True)

    return project_m1_data