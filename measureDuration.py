import boto3
import os
import time
import json
import pandas as pd
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

LAMBDA_FUNCTION_NAMES = [
   # Add your Lambda function names here
]
REGION = 'us-east-1'
MAX_INVOCATIONS = 100
SLEEP_TIME_FOR_INVOCATION = 1
WAIT_TIME_BETWEEN_PHASES = 300

lambda_client = boto3.client('lambda', region_name=REGION)
logs_client = boto3.client('logs', region_name=REGION)

test_function = {}

def convert_to_ist(utc_timestamp):
    utc_time = datetime.fromtimestamp(utc_timestamp, pytz.utc)
    ist_time = utc_time.astimezone(pytz.timezone('Asia/Kolkata'))
    return ist_time

def invoke_lambda(function_name, counter):
    payload = json.dumps({'counter': counter})
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',
        Payload=payload
    )
    print(f"Lambda {function_name} invoked with counter {counter}")
    return response['Payload'].read()

def update_lambda_env(function_name, counter):
    response = lambda_client.get_function_configuration(FunctionName=function_name)
    env_variables = response['Environment']['Variables']
    env_variables['NR_LAMBDA_COUNT'] = str(counter)
    lambda_client.update_function_configuration(
        FunctionName=function_name,
        Environment={'Variables': env_variables}
    )
    print(f"NR_LAMBDA_COUNT for {function_name} updated to {counter}")

def invoke_lambda_function(function_name):
    test_function[function_name] = {'start_time': None, 'end_time': None, 'query_results': []}

    counter = 0
    test_function[function_name]['start_time'] = time.time()
    while counter < MAX_INVOCATIONS:
        invoke_lambda(function_name, counter)
        update_lambda_env(function_name, counter)
        counter += 1
        time.sleep(SLEEP_TIME_FOR_INVOCATION)
    test_function[function_name]['end_time'] = time.time()
    print(f"Cold start phase for {function_name} completed.")
    
    time.sleep(WAIT_TIME_BETWEEN_PHASES)
    
    start_time_ist = convert_to_ist(test_function[function_name]['start_time'])
    end_time_ist = convert_to_ist(test_function[function_name]['end_time'])

    print(f"###########")
    print(f"Cold Start for {function_name}")
    print(f"Invocation period in IST:")
    print(f"Start time (IST): {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time (IST): {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")

    test_function[function_name]['query_results'] = query_cloudwatch_logs(function_name, test_function[function_name]['start_time'], test_function[function_name]['end_time'], cold_start=True)
    save_to_csv('./test-results/coldStart.csv', test_function[function_name]['query_results'])

    counter = 0
    test_function[function_name]['start_time'] = time.time()
    while counter < MAX_INVOCATIONS:
        invoke_lambda(function_name, counter)
        counter += 1
        time.sleep(SLEEP_TIME_FOR_INVOCATION)
    test_function[function_name]['end_time'] = time.time()
    print(f"Warm start phase for {function_name} completed.")
    
    time.sleep(WAIT_TIME_BETWEEN_PHASES)

    start_time_ist = convert_to_ist(test_function[function_name]['start_time'])
    end_time_ist = convert_to_ist(test_function[function_name]['end_time'])

    print(f"Warm Start for {function_name}")
    print(f"Invocation period in IST:")
    print(f"Start time (IST): {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time (IST): {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")

    test_function[function_name]['query_results'] = query_cloudwatch_logs(
        function_name, test_function[function_name]['start_time'], test_function[function_name]['end_time'], cold_start=False
    )
    save_to_csv('./test-results/warmStart.csv', test_function[function_name]['query_results'])
    
def query_cloudwatch_logs(function_name, start_time, end_time, cold_start):
    start_time_millis = int(start_time * 1000)
    end_time_millis = int(end_time * 1000)

    log_group_name = f'/aws/lambda/{function_name}'
    
    if cold_start:
        query_string = """
          fields @timestamp, @requestId, @initDuration, @billedDuration, @duration, @memorySize
          | filter @type = "REPORT"
          | filter ispresent(@initDuration)
          | sort @timestamp asc
          | limit 10000
          | stats 
             count(@initDuration) as coldStartCount,
             pct(@initDuration, 50) as p50Init,
             pct(@initDuration, 90) as p90Init,
             pct(@initDuration, 99) as p99Init,
             count(@billedDuration) as totalInvocations,
             avg(@billedDuration) as avgBilledDuration,
             min(@billedDuration) as minBilledDuration,
             max(@billedDuration) as maxBilledDuration,
             percentile(@billedDuration, 50) as p50BilledDuration,
             percentile(@billedDuration, 95) as p95BilledDuration,
             percentile(@billedDuration, 99) as p99BilledDuration
        """
    else:
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

    query_id = logs_client.start_query(
        logGroupName=log_group_name,
        startTime=start_time_millis,
        endTime=end_time_millis,
        queryString=query_string
    )['queryId']

    print(f"Query {query_id} started for log group {log_group_name}")
    
    query_results_data = []

    while True:
        try:
            query_res = logs_client.get_query_results(queryId=query_id)
            
            if query_res['status'] == 'Complete':
                print(f"Query {query_id} complete")
                
                for res in query_res['results']:
                    result_dict = {field['field']: field['value'] for field in res}
                    result_dict['FunctionName'] = function_name
                    query_results_data.append(result_dict)
                break
            
            elif query_res['status'] in ['Failed', 'Cancelled']:
                print(f"Query {query_id} {query_res['status']}")
                break
            
            time.sleep(1)
        except ClientError as e:
            print(f"Error retrieving query results: {e}")
            break
    
    print("Query results retrieved for:", function_name)
    print("##########")

    return query_results_data

def save_to_csv(file_path, data):
    try:
        df = pd.DataFrame(data)

        if 'FunctionName' in df.columns:
            columns_order = ['FunctionName'] + [col for col in df.columns if col != 'FunctionName']
            df = df[columns_order]

        file_exists = os.path.isfile(file_path)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        df.to_csv(file_path, mode='a', index=False, header=not file_exists)
        
        print(f"Data appended to {file_path}")
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def run_parallel_invocations():
    if os.path.exists('./test-results'):
        for file in os.listdir('./test-results'):
            os.remove(os.path.join('./test-results', file))

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(invoke_lambda_function, fn_name): fn_name for fn_name in LAMBDA_FUNCTION_NAMES}
        for future in as_completed(futures):
            fn_name = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred with {fn_name}: {e}")

if __name__ == "__main__":
    print("Starting parallel invocation of Lambda functions...")
    run_parallel_invocations()
    print("All Lambda functions have been invoked successfully.")
