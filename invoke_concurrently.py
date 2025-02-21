import boto3
import concurrent.futures
import time
import statistics
import argparse
import pandas as pd
from tabulate import tabulate

def invoke_lambda(lambda_client, function_arn, i):
    try:
        start_time = time.time()
        response = lambda_client.invoke(
            FunctionName=function_arn,
            InvocationType='RequestResponse',
            Payload=b'{}'  # Modify this based on your Lambda function's input
        )
        end_time = time.time()
        response_time = end_time - start_time
        return response['StatusCode'], response_time
    except Exception as e:
        print(f"Error invoking Lambda {i}: {e}")
        return None, None

def invoke_in_parallel(lambda_client, function_arn, concurrent_users, duration):
    start_time = time.time()
    response_times = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_users) as executor:
        while time.time() - start_time < duration:
            futures = [executor.submit(invoke_lambda, lambda_client, function_arn, i) for i in range(concurrent_users)]
            for future in concurrent.futures.as_completed(futures):
                status_code, response_time = future.result()
                if status_code == 200 and response_time is not None:
                    response_times.append(response_time)

    return response_times

def calculate_statistics(response_times):
    avg_response_time = statistics.mean(response_times)
    median_response_time = statistics.median(response_times)
    p50 = statistics.median(response_times)
    p95 = statistics.quantiles(response_times, n=100)[94]
    p99 = statistics.quantiles(response_times, n=100)[98]
    return avg_response_time, median_response_time, p50, p95, p99

def main(function_arn, concurrent_users, duration, output_file):
    lambda_client = boto3.client('lambda')
    response_times = invoke_in_parallel(lambda_client, function_arn, concurrent_users, duration)
    if response_times:
        avg, median, p50, p95, p99 = calculate_statistics(response_times)
        
        df = pd.DataFrame({
            "Concurrent Users": [concurrent_users],
            "Avg Response Time": [avg],
            "Median": [median],
            "P50": [p50],
            "P95": [p95],
            "P99": [p99]
        })
        
        print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
        
        if output_file:
            df.to_csv(output_file, index=False)
            print(f"Results saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Invoke AWS Lambda function with concurrency options.')
    parser.add_argument('--function_arn', type=str, required=True, help='ARN of the Lambda function to invoke')
    parser.add_argument('--concurrent_users', type=int, required=True, help='Number of concurrent users')
    parser.add_argument('--duration', type=int, required=True, help='Duration to run the invocations (in seconds)')
    parser.add_argument('--output_file', type=str, help='Output CSV file name to save results')

    args = parser.parse_args()

    main(args.function_arn, args.concurrent_users, args.duration, args.output_file)