import datetime as dt
import configparser
import logging
import logging.config
import google.auth.transport.requests
from pathlib import Path
from google.oauth2 import service_account
from api.fetch_airflow_data import fetch_dags_list, fetch_dags_execution
from api.data_processing import clean_data, influx_load_data


# Set root directory for every file path
ROOT_DIR = Path(__file__).resolve().parent.parent


# Configurator used for credentials
def setup_config():
    # Create ConfigParser Object
    config = configparser.ConfigParser()
    config.read(ROOT_DIR/'config'/'config.ini')

    return config


def main():
    logger.info('Start execution...')
                
    # Set GCP credentials file
    credentials = service_account.Credentials.from_service_account_file(
        ROOT_DIR/'config'/'gcp-service-account.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    try:
        # Get GCP token for authentication
        token_req = google.auth.transport.requests.Request()
        credentials.refresh(token_req)
        token = credentials.token

        # One time at day (22 o'clock), extract the list of DAGs into xlsx file, for new update
        if dt.datetime.now().hour == 22 and dt.datetime.now().minute == 00:
            fetch_dags_list(ROOT_DIR, config, logger, token)
        else:
            pass

        # 1. Fecth DAGs executions info
        # 2. Process and normalize data
        # 3. Load data into influx
        df_exec_list = fetch_dags_execution(ROOT_DIR, config, logger, token)
        df_cleaned_list = clean_data(logger, df_exec_list)
        influx_load_data(ROOT_DIR, config, logger, df_cleaned_list)

        logger.info('End execution.')

    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    logging.config.fileConfig(ROOT_DIR/'log'/'logging.conf')   # Load logger configuration
    logger = logging.getLogger('console_logger')    # Get configured logger
    config = setup_config()                         # Setup config for credentials
    main()
