
import time
from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook   
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def get_import_summary(endpoint:str, http_hook) -> dict:
    """
        Get the import summary when the import process finish
        Args:
            **kwargs:

        Returns:
            dict
    """
    try:
        # for res in import_response:
        with http_hook.run(endpoint= endpoint) as r:
            r.raise_for_status()
            if r.status_code == 200:
                response = r.json()
                response.pop('importOptions')
        return  response

    except Exception as e:
        print('fail to check import completness')
        raise e


@dag(template_searchpath="/opt/airflow/include/sql")
def bahmni_dhis_integration():
    @task
    def fetch_bahmni_data():
        file_name = "/opt/airflow/include/data/bahmni_opd_report.csv"
        mysql_hook = MySqlHook(mysql_conn_id="bahmni_db")
        sql_file_path = '/opt/airflow/include/sql/opd_report.sql'
    
        # Read SQL query from file
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()
        
        print(f"Executing query from: {sql_file_path}")
        #sql = "SELECT patient_id, name, age, last_visit FROM patients WHERE last_visit >= CURDATE() - INTERVAL 1 DAY;"
        data = mysql_hook.get_pandas_df(sql_query)

        data.to_csv(file_name, index=False)

        return file_name


    
    
    @task
    def import_to_dhis(file_path: str):
        http_hook = HttpHook(
            method='POST', 
            http_conn_id=f"ls_conn"
        )
        dataset_api = "api/dataValueSets?orgUnitIdScheme=name&categoryOptionComboIdScheme=name&preheatCache=true&async=true"

        try:
            import pandas as pd
            import json

            data = pd.read_csv(file_path, dtype=object)
            print(f"############ has {len(data)} records ############")
            headers = {'content-type': 'application/csv; charset=utf-8'}
            payload = data.to_csv(index=False)
            response = http_hook.run(
                endpoint= dataset_api,
                data=payload.encode(
                    'utf-8'),
                headers=headers
            )
            response.raise_for_status()
            print(json.loads(response.text))
                
            _status_link = json.loads(response.text)["response"]["relativeNotifierEndpoint"]
            _status_summmary_link = _status_link.replace("tasks", "taskSummaries")
            import_status={
                'import_status': _status_link,
                'import_summary': _status_summmary_link
            }
        except Exception as e:
            print("Operation failure: upload for province")
            raise e

        return import_status
    
    @task
    def check_import_completness(import_response) -> list:
        """
            Check the import completness and return the Import summary
            Args:
                **kwargs:

            Returns:
                list
        """
        
        print('check the import completness and return the Import summary')
        
        import_summary: list = []
        http_hook = HttpHook(method='GET', http_conn_id=f"ls_conn")
        try:
            check_completness = False
            while not check_completness:
                with http_hook.run(endpoint=import_response['import_status']) as r:
                    r.raise_for_status()
                    if r.status_code == 200:
                        response = r.json()
                        if response[0]['completed']:
                            print(response[0]['message'])
                            import_summary.append(get_import_summary(import_response['import_summary'], http_hook))
                            check_completness = True
                if not check_completness:
                    print(response[0]['message'])
                    time.sleep(60)

        except Exception as e:
            print('fail to check import completness')
            raise e
        
        return import_summary

    bahmni_data_file = fetch_bahmni_data()
    import_to_dhis = import_to_dhis(bahmni_data_file)
    import_status = check_import_completness(import_to_dhis)

    bahmni_data_file >> import_to_dhis >> import_status

bahmni_dhis_integration()
