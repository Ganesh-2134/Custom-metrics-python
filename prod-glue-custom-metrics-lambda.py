import boto3
import datetime
import json
import pytz

# Initialize AWS SDK clients
glue_client = boto3.client('glue')
cloudwatch_client = boto3.client('cloudwatch')

# Define the CloudWatch namespace
CLOUDWATCH_NAMESPACE = 'GlueCM'

def round_to_nearest_half_hour(dt):
    # Round to the nearest half-hour (00 or 30)
    minute = dt.minute
    if minute < 15:
        dt = dt.replace(minute=0, second=0, microsecond=0)
    elif minute < 45:
        dt = dt.replace(minute=30, second=0, microsecond=0)
    else:
        dt = dt.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    return dt

def lambda_handler(event, context):
    # Get the current time in UTC
    utc_tz = pytz.utc
    current_time = datetime.datetime.now(utc_tz)

    # Round the current time to the nearest half-hour
    end_time = round_to_nearest_half_hour(current_time)
    start_time = end_time - datetime.timedelta(minutes=60)  # One hour before the rounded time

    # Print the rounded-off start and end times
    print(f"Current Time: {current_time}")
    print(f"Rounded Start Time: {start_time}")
    print(f"Rounded End Time: {end_time}")

    # Initialize a dictionary to hold counts for each job
    job_counts = {}

    # List all Glue jobs
    glue_jobs = glue_client.get_jobs()

    # Iterate through each job to get the job runs
    for job in glue_jobs['Jobs']:
        job_name = job['Name']
        
        # Initialize counts for the current job
        job_counts[job_name] = {
            'Running': 0,
            'Succeeded': 0,
            'Failed': 0
        }
        
        # Get job runs for the last rounded hour
        job_runs = glue_client.get_job_runs(JobName=job_name)
        
        for run in job_runs['JobRuns']:
            # Ensure the StartedOn time is timezone-aware
            if 'StartedOn' in run:
                started_on = run['StartedOn'].astimezone(utc_tz)  # Convert to UTC timezone if needed
                
                # Check if the job run was started within the rounded time frame
                if start_time <= started_on <= end_time:
                    status = run['JobRunState']
                    if status == 'RUNNING':
                        job_counts[job_name]['Running'] += 1
                    elif status == 'SUCCEEDED':
                        job_counts[job_name]['Succeeded'] += 1
                    elif status == 'FAILED':
                        job_counts[job_name]['Failed'] += 1

    # Prepare CloudWatch metrics
    metric_data = []
    for job_name, counts in job_counts.items():
        metric_data.append({
            'MetricName': 'JobStatusCount',
            'Value': counts['Running'],
            'Unit': 'Count',
            'Dimensions': [
                {
                    'Name': 'JobName',
                    'Value': job_name
                },  
                {
                    'Name': 'Status',
                    'Value': 'Running'
                }
            ]
        })
        metric_data.append({
            'MetricName': 'JobStatusCount',
            'Value': counts['Succeeded'],
            'Unit': 'Count',
            'Dimensions': [
                {
                    'Name': 'JobName',
                    'Value': job_name
                },
                {
                    'Name': 'Status',
                    'Value': 'Succeeded'
                }
            ]
        })
        metric_data.append({
            'MetricName': 'JobStatusCount',
            'Value': counts['Failed'],
            'Unit': 'Count',
            'Dimensions': [
                {
                    'Name': 'JobName',
                    'Value': job_name
                },
                {
                    'Name': 'Status',
                    'Value': 'Failed'
                }
            ]
        })

    # Send metrics to CloudWatch
    cloudwatch_client.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=metric_data
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(job_counts)  # Return the counts for each job
    }
