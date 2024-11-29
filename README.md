AWS Glue Monitoring Lambda Functions
Overview
This repository contains Lambda functions for monitoring AWS Glue jobs and reporting their statuses to Amazon CloudWatch. The Lambda functions track various job statuses (Running, Succeeded, Failed, Canceled), calculate success rates, and push metrics to CloudWatch for monitoring and alerting.

Lambda Functions Included:
Job Status Count Reporting: This function retrieves Glue job runs, counts them by their status (Running, Succeeded, Failed), and pushes these counts as metrics to CloudWatch.

Hourly Glue Job Run Report: This function calculates the number of total runs, successful runs, failed runs, and running jobs for the past hour, then pushes these metrics to CloudWatch.

Job Status Count and Metrics Aggregation: Similar to the first function but with aggregated reporting and CloudWatch metrics for multiple Glue jobs.

Glue Job Run Success Rate Reporting: This function calculates the success rate of Glue jobs for the last hour and pushes relevant metrics to CloudWatch.

Prerequisites
AWS Account: You need an AWS account with the necessary permissions to access Glue jobs and CloudWatch.
IAM Role for Lambda: The Lambda function must have a role with the following permissions:
glue:GetJobs
glue:GetJobRuns
cloudwatch:PutMetricData
Python 3.x: The Lambda function is written in Python, and you should use an environment that supports Python 3.x.
Setup Instructions
Create the Lambda Function:

Go to the AWS Lambda console and create a new Lambda function.
Choose Python 3.x as the runtime.
Use the provided code for each respective Lambda function. Ensure that the code is copied correctly into the function editor.
Assign IAM Role:

Create an IAM role with the necessary permissions mentioned above.
Attach the IAM role to the Lambda function.
Set up CloudWatch:

After the Lambda function is triggered, it will automatically push the Glue job metrics to CloudWatch. You can monitor these metrics in the CloudWatch console.
Configure Event Trigger (Optional):

You can set up CloudWatch Events to trigger the Lambda function on a regular schedule (e.g., every hour). This is useful for periodic job monitoring.
Function Descriptions
1. Job Status Count Reporting (lambda_handler - Function 1)
This function fetches all AWS Glue jobs and their respective job runs for the past hour. It categorizes job runs by their statuses (Running, Succeeded, Failed) and pushes these counts to CloudWatch.

Key Actions:
List all Glue jobs and their runs.
Filter job runs based on their start time (within the past hour).
Categorize job runs by their status.
Send counts of job runs (Running, Succeeded, Failed) to CloudWatch.
2. Hourly Glue Job Run Report (lambda_handler - Function 2)
This function calculates and reports the total number of job runs, successful runs, failed runs, canceled jobs, and running jobs for the last hour. Metrics are pushed to CloudWatch.

Key Actions:
Fetch Glue job runs for the last hour.
Count job statuses (Total, Running, Canceled, Successful, Failed).
Calculate job run success rate.
Send job run data to CloudWatch.
3. Job Status Count and Metrics Aggregation (lambda_handler - Function 3)
This function aggregates job run counts across different Glue jobs and pushes CloudWatch metrics for Running, Succeeded, and Failed jobs.

Key Actions:
Retrieve Glue job data.
Aggregate job status counts.
Push aggregated metrics to CloudWatch.
4. Glue Job Run Success Rate Reporting (lambda_handler - Function 4)
This function calculates the success rate of Glue job runs within the past hour and reports this rate along with other job status counts to CloudWatch.

Key Actions:
Get all Glue job runs.
Count runs by status (Succeeded, Failed, Running, Canceled).
Calculate and push the success rate of the Glue job runs to CloudWatch.
Example CloudWatch Metrics
The Lambda function pushes the following metrics to CloudWatch under the namespace GlueCM:

Glue.TotalRuns: Total number of job runs in the last hour.
Glue.RunningJobs: Number of jobs that are currently running.
Glue.CanceledJobs: Number of jobs that were canceled.
Glue.SuccessfulRuns: Number of successful job runs.
Glue.FailedRuns: Number of failed job runs.
Glue.RunSuccessRate: Success rate of the job runs (as a percentage).
These metrics can be used for monitoring and triggering CloudWatch Alarms to alert on job failures or high success rates.

Notes
Time Zones: All times are UTC-aware, so ensure your CloudWatch settings align with UTC time.
Error Handling: The Lambda function does not contain complex error handling. It's recommended to add logging and error handling to production Lambda functions.
Pagination: Glue API responses are paginated. The code includes pagination handling for both job listings and job runs.
Troubleshooting
If the Lambda function fails to run or doesn't return the expected results, ensure that the IAM role attached to the Lambda function has the correct permissions.
Verify that the Glue jobs exist and are configured correctly.