import json
import boto3
import logging
import csv
import datetime
from botocore.exceptions import ClientError
from datetime import timezone  # Import timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
glue_client = boto3.client('glue')
cloudwatch_client = boto3.client('cloudwatch')  # CloudWatch client

# Cost Constants
cost_per_dpu_hour = 0.44  # USD for Glue 1.0
cost_per_worker_hour_g_1x = 0.44  # USD for G.1X worker type
cost_per_worker_hour_g_2x = 0.88  # USD for G.2X worker type

# Time Calculation
current_time = datetime.datetime.utcnow().replace(tzinfo=timezone.utc)  # Make current time aware
#twelve_hour_ago = current_time - datetime.timedelta(hours=12)  
one_hour_ago = current_time - datetime.timedelta(hours=1)  # This will also be aware

def get_all_job_run_ids():
    job_run_ids = []
    
    try:
        # Get a list of all Glue jobs
        response = glue_client.get_jobs()
        jobs = response['Jobs']

        # Loop through each job to get their run details
        for job in jobs:
            job_name = job['Name']
            # Fetch job runs for the job
            job_runs_response = glue_client.get_job_runs(JobName=job_name)
            for job_run in job_runs_response.get('JobRuns', []):
                started_on = job_run['StartedOn'].replace(tzinfo=timezone.utc)  # Make started_on aware
                if started_on >= one_hour_ago:  # Both should be aware now
                    job_run_ids.append({'job_name': job_name, 'job_run_id': job_run['Id']})

    except ClientError as e:
        logger.error(f"Error fetching job runs: {e}")
    
    return job_run_ids

def get_job_run_details(job_run_ids):
    job_metrics = {}  # Dictionary to hold aggregated metrics per job

    for job_run in job_run_ids:
        job_name = job_run['job_name']
        job_run_id = job_run['job_run_id']

        try:
            job_run_info = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']
            dpu_seconds = job_run_info.get('DPUSeconds', None)
            execution_time = job_run_info.get('ExecutionTime', 0)
            worker_type = job_run_info.get('WorkerType', 'Standard')

            if dpu_seconds is not None:
                if worker_type == 'G.1X':
                    job_cost = cost_per_worker_hour_g_1x * (dpu_seconds / 3600)
                elif worker_type == 'G.2X':
                    job_cost = cost_per_worker_hour_g_2x * (dpu_seconds / 3600)
                else:
                    job_cost = cost_per_dpu_hour * (dpu_seconds / 3600)
            else:
                allocated_capacity = job_run_info.get('AllocatedCapacity', 0)
                dpu_seconds = execution_time * allocated_capacity
                if worker_type == 'G.1X':
                    job_cost = cost_per_worker_hour_g_1x * (execution_time / 3600) * allocated_capacity
                elif worker_type == 'G.2X':
                    job_cost = cost_per_worker_hour_g_2x * (execution_time / 3600) * allocated_capacity
                else:
                    job_cost = cost_per_dpu_hour * (execution_time / 3600) * allocated_capacity
            
            # Aggregate metrics for the job
            if job_name not in job_metrics:
                job_metrics[job_name] = {'DPUSeconds': 0, 'Cost': 0}

            job_metrics[job_name]['DPUSeconds'] += dpu_seconds
            job_metrics[job_name]['Cost'] += round(job_cost, 2)

        except ClientError as e:
            logger.error(f"Error fetching job run details for {job_run_id}: {e}")

    # Now put metrics to CloudWatch for each job
    for job_name, metrics in job_metrics.items():
        cloudwatch_client.put_metric_data(
            Namespace='GlueCM',
            MetricData=[
                {
                    'MetricName': 'DPU_Seconds',
                    'Dimensions': [
                        {
                            'Name': 'JobName',
                            'Value': job_name
                        }
                    ],
                    'Value': metrics['DPUSeconds'],
                    'Unit': 'Seconds'
                },
                {
                    'MetricName': 'DPU_Cost',
                    'Dimensions': [
                        {
                            'Name': 'JobName',
                            'Value': job_name
                        }
                    ],
                    'Value': metrics['Cost'],
                    'Unit': 'None'  # Cost doesn't have a specific unit
                }
            ]
        )

    logger.info(f"Collected details for {len(job_metrics)} jobs.")
    return job_metrics

def lambda_handler(event, context):
    job_run_ids = get_all_job_run_ids()
    if job_run_ids:
        job_run_details = get_job_run_details(job_run_ids)
    else:
        logger.info("No job run IDs found in the last twelve hours.")
