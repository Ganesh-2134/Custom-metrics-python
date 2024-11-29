import boto3
from datetime import datetime, timedelta, timezone

cloudwatch = boto3.client('cloudwatch')
glue = boto3.client('glue')

def lambda_handler(event, context):
    # Get the current time and time 1 hour ago (make them UTC-aware)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)
    
    # Initialize counters
    total_runs = 0
    running = 0
    canceled = 0
    successful_runs = 0
    failed_runs = 0
    
    # List all Glue jobs with pagination
    glue_jobs = []
    next_token = None
    
    while True:
        if next_token:
            response = glue.get_jobs(NextToken=next_token)
        else:
            response = glue.get_jobs()

        glue_jobs.extend(response['Jobs'])
        next_token = response.get('NextToken')
        
        if not next_token:
            break
    
    # Iterate through each Glue job
    for job in glue_jobs:
        job_name = job['Name']
        
        # Get job run details with pagination
        job_runs = []
        next_token = None
        
        while True:
            if next_token:
                response = glue.get_job_runs(JobName=job_name, NextToken=next_token)
            else:
                response = glue.get_job_runs(JobName=job_name)
            
            job_runs.extend(response['JobRuns'])
            next_token = response.get('NextToken')
            
            if not next_token:
                break
        
        # Process each job run
        for job_run in job_runs:
            # Ensure the job run time is also UTC-aware
            job_run_started_on = job_run['StartedOn'].replace(tzinfo=timezone.utc)
            
            # Filter runs in the last hour
            if start_time <= job_run_started_on <= end_time:
                total_runs += 1
                status = job_run['JobRunState']
                
                # Count statuses
                if status == 'SUCCEEDED':
                    successful_runs += 1
                elif status == 'FAILED':
                    failed_runs += 1
                elif status == 'RUNNING':
                    running += 1
                elif status == 'CANCELED':
                    canceled += 1
    
    # Calculate success rate
    run_success_rate = (successful_runs / total_runs) * 100 if total_runs > 0 else 0
    
    # Push metrics to CloudWatch
    push_metrics_to_cloudwatch(total_runs, running, canceled, successful_runs, failed_runs, run_success_rate)
    
    return {
        'statusCode': 200,
        'body': 'Report generated and metrics pushed to CloudWatch'
    }

def push_metrics_to_cloudwatch(total_runs, running, canceled, successful_runs, failed_runs, run_success_rate):
    current_time = datetime.now(timezone.utc)  # Get current time in UTC
    
    cloudwatch.put_metric_data(
        Namespace='GlueCM',
        MetricData=[
            {
                'MetricName': 'Glue.TotalRuns',
                'Timestamp': current_time,
                'Value': total_runs,
                'Unit': 'Count'
            },
            {
                'MetricName': 'Glue.RunningJobs',
                'Timestamp': current_time,
                'Value': running,
                'Unit': 'Count'
            },
            {
                'MetricName': 'Glue.CanceledJobs',
                'Timestamp': current_time,
                'Value': canceled,
                'Unit': 'Count'
            },
            {
                'MetricName': 'Glue.SuccessfulRuns',
                'Timestamp': current_time,
                'Value': successful_runs,
                'Unit': 'Count'
            },
            {
                'MetricName': 'Glue.FailedRuns',
                'Timestamp': current_time,
                'Value': failed_runs,
                'Unit': 'Count'
            },
            {
                'MetricName': 'Glue.RunSuccessRate',
                'Timestamp': current_time,
                'Value': run_success_rate,
                'Unit': 'Percent'
            }
        ]
    )
