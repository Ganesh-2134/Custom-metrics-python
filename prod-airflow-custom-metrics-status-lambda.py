import boto3
import requests
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os

# Hardcoded environment details
REGION = "ap-southeast-2"
ENV_NAMES = json.loads(os.getenv("ENV_NAMES", "[]"))  # Load ENV_NAMES from Lambda environment variable
MAX_WORKERS = 10  # Number of parallel workers

# Initialize CloudWatch client
cloudwatch = boto3.client('cloudwatch', region_name=REGION)

def get_session_info(region, env_name):
    logging.basicConfig(level=logging.INFO)
    try:
        mwaa = boto3.client('mwaa', region_name=region)
        response = mwaa.create_web_login_token(Name=env_name)
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]

        login_url = f"https://{web_server_host_name}/aws_mwaa/login"
        login_payload = {"token": web_token}
        response = requests.post(login_url, data=login_payload, timeout=10)

        if response.status_code == 200:
            return web_server_host_name, response.cookies["session"]
        else:
            logging.error("Failed to log in: HTTP %d", response.status_code)
            return None, None
    except requests.RequestException as e:
        logging.error("Request failed: %s", str(e))
        return None, None
    except Exception as e:
        logging.error("An unexpected error occurred: %s", str(e))
        return None, None

def list_dags(region, env_name):
    logging.info(f"Listing all DAGs in environment {env_name} at region {region}")

    web_server_host_name, session_cookie = get_session_info(region, env_name)
    if not session_cookie:
        logging.error("Authentication failed, no session cookie retrieved.")
        return None

    cookies = {"session": session_cookie}
    url = f"https://{web_server_host_name}/api/v1/dags?limit=300"  # API call with limit 300

    all_dags = []
    while url:  # Pagination logic to handle large number of DAGs
        try:
            response = requests.get(url, cookies=cookies)
            if response.status_code == 200:
                data = response.json()
                all_dags.extend(data['dags'])  # Append newly fetched DAGs
                url = data.get('_links', {}).get('next', {}).get('href', None)  # Set URL to the next page of results
                if url:
                    url = f"https://{web_server_host_name}{url}"  # Ensure the full URL is used
                else:
                    break
            else:
                logging.error(f"Failed to fetch DAGs: HTTP {response.status_code} - {response.text}")
                break
        except requests.RequestException as e:
            logging.error(f"Request failed: {str(e)}")
            break

    logging.info(f"Fetched a total of {len(all_dags)} DAGs.")
    return all_dags

def list_dag_runs(region, env_name, dag_id, start_time_str, end_time_str):
    logging.info(f"Listing DAG runs for DAG {dag_id} in environment {env_name} at region {region}")

    web_server_host_name, session_cookie = get_session_info(region, env_name)
    if not session_cookie:
        logging.error("Authentication failed, no session cookie retrieved.")
        return None

    cookies = {"session": session_cookie}
    url = f"https://{web_server_host_name}/api/v1/dags/{dag_id}/dagRuns?start_date_gte={start_time_str}&end_date_lte={end_time_str}"

    try:
        response = requests.get(url, cookies=cookies)
        if response.status_code == 200:
            return response.json().get("dag_runs", [])
        else:
            logging.error(f"Failed to fetch DAG runs for {dag_id}: HTTP {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        logging.error(f"Request to fetch DAG runs failed for {dag_id}: {str(e)}")
        return None

def put_cloudwatch_metrics(success_count, failed_count, dag_id, env_name, env_success_count, env_failed_count):
    try:
        cloudwatch.put_metric_data(
            Namespace='AmazonMWAA',
            MetricData=[
                {
                    'MetricName': 'DAGRuns.Success',
                    'Dimensions': [
                        {'Name': 'EnvironmentName', 'Value': env_name},
                        {'Name': 'DAG_ID', 'Value': dag_id}
                    ],
                    'Value': success_count,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'DAGRuns.Failed',
                    'Dimensions': [
                        {'Name': 'EnvironmentName', 'Value': env_name},
                        {'Name': 'DAG_ID', 'Value': dag_id}
                    ],
                    'Value': failed_count,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'DAGRuns.EnvironmentSuccess',
                    'Dimensions': [{'Name': 'EnvironmentName', 'Value': env_name}],
                    'Value': env_success_count,
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'DAGRuns.EnvironmentFailed',
                    'Dimensions': [{'Name': 'EnvironmentName', 'Value': env_name}],
                    'Value': env_failed_count,
                    'Unit': 'Count'
                }
            ]
        )
        logging.info(f"Successfully published CloudWatch metrics for {dag_id}: {success_count} successes, {failed_count} failures, {env_success_count} total environment successes, {env_failed_count} total environment failures.")
    except Exception as e:
        logging.error(f"Failed to put CloudWatch metrics for {dag_id}: {str(e)}")

def fetch_all_dag_runs(dags, region, env_name, start_time_str, end_time_str):
    all_dag_runs = []
    success_count = 0
    failed_count = 0
    env_success_count = 0
    env_failed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(list_dag_runs, region, env_name, dag["dag_id"], start_time_str, end_time_str): dag["dag_id"] for dag in dags}

        for future in as_completed(futures):
            dag_id = futures[future]
            try:
                dag_runs = future.result()
                if dag_runs:
                    logging.info(f"Fetched {len(dag_runs)} runs for DAG {dag_id}.")
                    for run in dag_runs:
                        run['environment'] = env_name  # Add environment name to each run
                        # Keep only required fields
                        run_filtered = {
                            'dag_id': run.get('dag_id'),
                            'execution_date': run.get('execution_date'),
                            'external_trigger': run.get('external_trigger'),
                            'start_date': run.get('start_date'),
                            'state': run.get('state'),
                            'environment': run['environment']  # Include the environment
                        }
                        all_dag_runs.append(run_filtered)

                    # Count successful and failed DAG runs
                    for run in dag_runs:
                        if run.get('state') == 'success':
                            success_count += 1
                            env_success_count += 1
                        elif run.get('state') == 'failed':
                            failed_count += 1
                            env_failed_count += 1

                    # Send CloudWatch metrics for this DAG
                    put_cloudwatch_metrics(success_count, failed_count, dag_id, env_name, env_success_count, env_failed_count)
            except Exception as e:
                logging.error(f"Error fetching DAG runs for {dag_id}: {str(e)}")

    return all_dag_runs

def lambda_handler(event, context):
    logging.basicConfig(level=logging.INFO)

    region = REGION
    all_dag_runs = []

    # Iterate through each environment
    for env_name in ENV_NAMES:
        dags = list_dags(region, env_name)
        if dags:
            start_time = datetime.now() - timedelta(minutes=30)  # Adjust for last 30 mins
            end_time = datetime.now()
            start_time_str = start_time.isoformat() + "Z"  # Convert to ISO 8601 format
            end_time_str = end_time.isoformat() + "Z"  # Convert to ISO 8601 format
            
            # Fetch all DAG runs for the current environment
            dag_runs = fetch_all_dag_runs(dags, region, env_name, start_time_str, end_time_str)
            all_dag_runs.extend(dag_runs)

    return {
        'statusCode': 200,
        'body': json.dumps('DAG runs processed successfully!')
    }
