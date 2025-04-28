import requests
import json
import pandas as pd
import datetime
import pytz
from datetime import timedelta


# This is function used to fetch all DAGs list from Airflow
def fetch_dags_list(ROOT_DIR, config, logger, token):
    # Set headers for API request
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        logger.info('I\'m exporting DAGs list...')

        # Offset variable is used for the dynamic creation of endpoint. This variable is necessary
        # to extract all DAGs list, because the Airflow API get only 100 rows by default.
        offset = 0  # Initialize offset (counter of the loop)
        df = pd.DataFrame()  # Initialize dataframe

        dags_list_url = config['airflow']['dags_list_url']

        while True:
            endpoint = dags_list_url + str(offset)
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 200:
                data = response.json()
                temp_df = pd.json_normalize(data["dags"])
                df = pd.concat([df, temp_df], ignore_index=True)

                total = data["total_entries"]
                offset += 100

                if temp_df.empty or offset >= total:
                    break

            else:
                logger.error(response.status_code)
                break

        # Check if dataframe is not empty
        if not df.empty:
            df = df.astype(str)  # Convert all data into string
            # Remove non-ownership dags from the list
            df = df[~df["dag_id"].str.contains("airflow_monitoring|CLEANUP_POSTGRES_AIRFLOW", regex=True)]
            df = df[~df["tags"].str.contains("Data Platform")]

            # Export only dag_id without other informations
            df = df["dag_id"]
            df.to_excel(ROOT_DIR/'data'/'DAGs_list.xlsx', index=False)
            logger.info('DAGs list exported.')

        else:
            logger.warning('The DAGs list is empty!')

    except Exception as e:
        logger.error(e)


# This is function used to fetch the DAGs exectuions informations from Airflow
def fetch_dags_execution(ROOT_DIR, config, logger, token):
    # Calculate delta for consider a time interval
    # It's necessary to convert the time in UTC, because is the standard of Airflow
    delta_time_utc = (datetime.datetime.now(pytz.utc) - timedelta(hours=2)).isoformat()

    df = pd.read_excel(ROOT_DIR/'data'/'DAGs_list.xlsx')
    dags = df["dag_id"].to_list()

    # Set headers for
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "order_by": "-dag_run_id",
        "start_date_gte": delta_time_utc,
        "dag_ids": dags,
    }

    try:
        logger.info('I\'m exporting executions list...')

        offset = 0
        df = pd.DataFrame()

        dags_list_url = config['airflow']['dags_exec_url']

        while True:
            endpoint = dags_list_url + str(offset)
            response = requests.post(
                endpoint, headers=headers, data=json.dumps(payload)
            )
            if response.status_code == 200:
                data = response.json()
                temp_df = pd.json_normalize(data["dag_runs"])
                df = pd.concat([df, temp_df], ignore_index=True)

                total = data["total_entries"]
                offset += 100

                if temp_df.empty or offset >= total:
                    break

            else:
                logger.error(response.status_code)
                break

        if not df.empty:
            logger.info('Execution list exported.')
            return df
        
        else:
            logger.warning('The execution list is empty!')

    except Exception as e:
        logger.error(e)
