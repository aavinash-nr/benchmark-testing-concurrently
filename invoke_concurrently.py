import boto3
import concurrent.futures
import time
import statistics
import argparse
import pandas as pd
import base64
import re
import pytz
from datetime import datetime
from botocore.exceptions import ClientError

# Constants
wait_time_before_query = 180

def invoke_lambda(lambda_client, function_arn, i):
    try:
        response = lambda_client.invoke(
            FunctionName=function_arn,
            InvocationType='RequestResponse',
            Payload=b'{}',
            LogType='Tail'
        )

        log_result = base64.b64decode(response['LogResult']).decode('utf-8')
        billed_duration = extract_billed_duration(log_result)

        if response['StatusCode'] == 200:
            print(f"[DEBUG] Invocation {i}: StatusCode=200, Billed Duration={billed_duration} ms")
        
        return response['StatusCode'], billed_duration
    except Exception as e:
        print(f"[ERROR] Error invoking Lambda {i}: {e}")
        return None, None

def extract_billed_duration(log):
    match = re.search(r'Billed Duration: (\d+) ms', log)
    return int(match.group(1)) if match else None

def invoke_in_parallel(lambda_client, function_arn, concurrent_users, duration):
    start_time = time.time()
    billed_durations = []
    total_requests = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_users) as executor:
        while time.time() - start_time < duration:
            futures = [executor.submit(invoke_lambda, lambda_client, function_arn, i) for i in range(concurrent_users)]
            total_requests += concurrent_users
            success_count = 0
            for future in concurrent.futures.as_completed(futures):
                status_code, billed_duration = future.result()
                if status_code == 200 and billed_duration is not None:
                    success_count += 1
                    billed_durations.append(billed_duration)

            print(f"[DEBUG] Cycle completed with {success_count} / {concurrent_users} successful invocations.")

    end_time = time.time()
    print(f"[INFO] Total requests sent: {total_requests}")
    print(f"[INFO] Successful requests: {len(billed_durations)}")
    return billed_durations, total_requests, start_time, end_time

def calculate_statistics(billed_durations):
    avg_billed_duration = statistics.mean(billed_durations)
    median_billed_duration = statistics.median(billed_durations)
    p50 = statistics.median(billed_durations)
    p95 = statistics.quantiles(billed_durations, n=100)[94]
    p99 = statistics.quantiles(billed_durations, n=100)[98]
    return avg_billed_duration, median_billed_duration, p50, p95, p99

def query_cloudwatch_logs(log_group_name, start_time, end_time):
    client = boto3.client('logs')
    query_string = """
    fields @timestamp, @requestId, @billedDuration, @duration, @memorySize
    | filter @type = "REPORT"
    | sort @timestamp asc
    | limit 10000
    | stats 
        count(@billedDuration) as totalInvocations,
        avg(@billedDuration) as avgBilledDuration,
        min(@billedDuration) as minBilledDuration,
        max(@billedDuration) as maxBilledDuration,
        percentile(@billedDuration, 50) as p50BilledDuration,
        percentile(@billedDuration, 95) as p95BilledDuration,
        percentile(@billedDuration, 99) as p99BilledDuration
    """

    max_retries = 60
    retry_count = 0
    
    start_query_response = client.start_query(
        logGroupName=log_group_name,
        startTime=int(start_time * 1000),
        endTime=int(end_time * 1000),
        queryString=query_string
    )

    query_id = start_query_response['queryId']
    query_statistics = {}

    print(f"Query started with ID: {query_id}, waiting for completion...")

    while retry_count < max_retries:
        try:
            query_results = client.get_query_results(queryId=query_id)
            status = query_results['status']
            
            if status == 'Complete':
                results = query_results.get('results', [])
                if results:
                    print("Query complete. Processing results...")
                    for result in results:
                        for field in result:
                            field_name = field.get('field')
                            field_value = field.get('value')
                            if field_name and field_value:
                                try:
                                    query_statistics[field_name] = float(field_value)
                                except ValueError:
                                    query_statistics[field_name] = field_value
                    break
                else:
                    print("Query returned no results.")
                    break
            elif status in ['Failed', 'Cancelled']:
                print(f"Query failed with status: {status}")
                break
                
            print(f"Query in progress... (attempt {retry_count + 1}/{max_retries})")
            time.sleep(5)
            retry_count += 1
            
        except ClientError as e:
            print(f"Error during query execution: {e}")
            break

    if retry_count >= max_retries:
        print("Query timed out after maximum retries")

    return query_statistics

def convert_to_ist(utc_timestamp):
    utc_time = datetime.fromtimestamp(utc_timestamp, pytz.utc)
    ist_time = utc_time.astimezone(pytz.timezone('Asia/Kolkata'))
    return ist_time

def main(function_arn, concurrent_users, duration, log_group_name, output_file):
    lambda_client = boto3.client('lambda')
    
    billed_durations, total_requests, start_time_utc, end_time_utc = invoke_in_parallel(
        lambda_client, function_arn, concurrent_users, duration
    )

    time.sleep(wait_time_before_query)

    start_time_ist = convert_to_ist(start_time_utc)
    end_time_ist = convert_to_ist(end_time_utc)
    
    print(f"Invocation period in IST:")
    print(f"Start time (IST): {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time (IST): {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")

    start_time_utc -= 5
    end_time_utc += 5

    try:
        query_statistics = query_cloudwatch_logs(log_group_name, start_time_utc, end_time_utc)

        if query_statistics: 
            print("\nCloudWatch Logs Insights Query Statistics:")
            print(f"Concurrent Users: {concurrent_users}")
            print(f"Total Invocations: {query_statistics.get('totalInvocations', 0)}")
            print(f"Average Billed Duration (ms): {query_statistics.get('avgBilledDuration', 0.0):.4f}")
            print(f"Minimum Billed Duration (ms): {query_statistics.get('minBilledDuration', 0.0):.4f}")
            print(f"Maximum Billed Duration (ms): {query_statistics.get('maxBilledDuration', 0.0):.4f}")
            print(f"50th Percentile Billed Duration (ms): {query_statistics.get('p50BilledDuration', 0.0):.4f}")
            print(f"95th Percentile Billed Duration (ms): {query_statistics.get('p95BilledDuration', 0.0):.4f}")
            print(f"99th Percentile Billed Duration (ms): {query_statistics.get('p99BilledDuration', 0.0):.4f}")

            # Prepare DataFrame for saving
            output_data = {
                'Statistic': [
                    'Concurrent Users',
                    'Total Invocations',
                    'Average Billed Duration (ms)',
                    'Minimum Billed Duration (ms)',
                    'Maximum Billed Duration (ms)',
                    '50th Percentile Billed Duration (ms)',
                    '95th Percentile Billed Duration (ms)',
                    '99th Percentile Billed Duration (ms)'
                ],
                'Value': [
                    concurrent_users,
                    query_statistics.get('totalInvocations', 0),
                    query_statistics.get('avgBilledDuration', 0.0),
                    query_statistics.get('minBilledDuration', 0.0),
                    query_statistics.get('maxBilledDuration', 0.0),
                    query_statistics.get('p50BilledDuration', 0.0),
                    query_statistics.get('p95BilledDuration', 0.0),
                    query_statistics.get('p99BilledDuration', 0.0)
                ]
            }

            df_results = pd.DataFrame(output_data)

            if output_file:
                df_results.to_csv(output_file, index=False)
                print(f"Results have been saved to {output_file}.")

        else:
            print("No invocation logs found for the specified time period and log group.")
    except Exception as e:
                print(f"Error querying CloudWatch Logs: {e}")

    if output_file:
        try:
            avg_billed_duration, median_billed_duration, p50, p95, p99 = calculate_statistics(billed_durations)
            
            df_detailed = pd.DataFrame({
                'Statistic': ['Average', 'Median', '50th Percentile', '95th Percentile', '99th Percentile'],
                'Duration (ms)': [avg_billed_duration, median_billed_duration, p50, p95, p99]
            })


            with open(output_file, 'a') as f:
                df_detailed.to_csv(f, index=False, header=f.tell()==0)

            print(f"Detailed statistics have been appended to {output_file}.")
        except Exception as e:
            print(f"Error writing detailed statistics to CSV file: {e}")

def extract_function_name_from_arn(arn):
    return arn.split(':')[-1]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Invoke AWS Lambda function with concurrency options.')
    parser.add_argument('--function_arn', type=str, required=True, help='ARN of the Lambda function to invoke')
    parser.add_argument('--concurrent_users', type=int, required=True, help='Number of concurrent users')
    parser.add_argument('--duration', type=int, required=True, help='Duration to run the invocations (in seconds)')
    parser.add_argument('--output_file', type=str, help='Output CSV file name to save results')

    args = parser.parse_args()
    function_name = extract_function_name_from_arn(args.function_arn)
    log_group_name = f"/aws/lambda/{function_name}"


    main(args.function_arn, args.concurrent_users, args.duration, log_group_name, args.output_file)