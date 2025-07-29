import argparse
import boto3
import requests
import os
import time
import json
import pandas as pd
import pytz
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

# Example: Lambda function names mapped to a list of layer ARNs
LAMBDA_FUNCTIONS_WITH_LAYERS = {
    # 'functionName': [
    #     'arn:aws:lambda:us-east-1:123456:layer:NRExample:1',
    # ],
}

REGION = 'us-east-1'
MAX_INVOCATIONS = 100
SLEEP_TIME_FOR_INVOCATION = 1
WAIT_TIME_BETWEEN_PHASES = 600

def get_layer_arn_for_runtime(runtime):
    url = f'https://{REGION}.layers.newrelic-external.com/get-layers'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        layers = data.get('Layers', [])
        for layer in layers:
            latest_version = layer.get('LatestMatchingVersion', {})
            compatible_runtimes = latest_version.get('CompatibleRuntimes', [])

            if runtime in compatible_runtimes:
                print(f"Found Layer ARN for {runtime}: {latest_version.get('LayerVersionArn')}")
                return latest_version.get('LayerVersionArn')

        print(f"No compatible layer found for runtime: {runtime}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while accessing the API: {e}")
        return None

def update_lambda_layer(function_name, layers_arn_list):
    lambda_client.update_function_configuration(
        FunctionName=function_name,
        Layers=layers_arn_list
    )
    print(f"Layers for {function_name} updated to {layers_arn_list}.")
    time.sleep(5)

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

def invoke_lambda_function(function_name, layers_list, path_to_save_csv, enable_cold_start=True, enable_warm_start=True, enable_prod_layer=True):
    function_configuration = lambda_client.get_function(FunctionName=function_name)
    runtime = function_configuration['Configuration']['Runtime']
    
    if enable_prod_layer:
        prod_layer_arn = get_layer_arn_for_runtime(runtime)
        if prod_layer_arn:
            print(f"Using layer ARN from API: {prod_layer_arn}")
            layers_list.append(prod_layer_arn)  # Add the produced layer, if obtained
        else:
            print(f"No production layer found for runtime: {runtime}")
    else:
        print(f"Production layer testing disabled for {function_name}")

    test_function[function_name] = {'start_time': None, 'end_time': None, 'query_results': []}
    
    # Test each layer configuration
    for layer_config in layers_list:
        update_lambda_layer(function_name, [layer_config])
        
        # Measure cold starts (only if enabled)
        if enable_cold_start:
            print(f"Starting cold start testing for {function_name} with layer {layer_config}")
            counter = 0
            test_function[function_name]['start_time'] = time.time()
            while counter < MAX_INVOCATIONS:
                invoke_lambda(function_name, counter)
                update_lambda_env(function_name, counter)
                counter += 1
                time.sleep(SLEEP_TIME_FOR_INVOCATION)
            test_function[function_name]['end_time'] = time.time()
            print(f"Cold start phase for {function_name} with layer {layer_config} completed.")
            time.sleep(WAIT_TIME_BETWEEN_PHASES)

            start_time_ist = convert_to_ist(test_function[function_name]['start_time'])
            end_time_ist = convert_to_ist(test_function[function_name]['end_time'])

            print(f"Cold Start for {function_name} with layer {layer_config}")
            print(f"Invocation period in IST:")
            print(f"Start time (IST): {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"End time (IST): {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")

            test_function[function_name]['query_results'] = query_cloudwatch_logs(
                function_name, test_function[function_name]['start_time'], test_function[function_name]['end_time'], cold_start=True
            )
            save_to_csv(
                os.path.join(path_to_save_csv, f'coldStart_{function_name}_{layer_config.split(":")[-1]}.csv'),
                test_function[function_name]['query_results']
            )
        else:
            print(f"Cold start testing disabled for {function_name}")

        # Measure warm starts (only if enabled)
        if enable_warm_start:
            print(f"Starting warm start testing for {function_name} with layer {layer_config}")
            counter = 0
            test_function[function_name]['start_time'] = time.time()
            while counter < MAX_INVOCATIONS:
                invoke_lambda(function_name, counter)
                counter += 1
                time.sleep(SLEEP_TIME_FOR_INVOCATION)
            test_function[function_name]['end_time'] = time.time()
            print(f"Warm start phase for {function_name} with layer {layer_config} completed.")
            time.sleep(WAIT_TIME_BETWEEN_PHASES)

            start_time_ist = convert_to_ist(test_function[function_name]['start_time'])
            end_time_ist = convert_to_ist(test_function[function_name]['end_time'])

            print(f"Warm Start for {function_name} with layer {layer_config}")
            print(f"Invocation period in IST:")
            print(f"Start time (IST): {start_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"End time (IST): {end_time_ist.strftime('%Y-%m-%d %H:%M:%S')}")

            test_function[function_name]['query_results'] = query_cloudwatch_logs(
                function_name, test_function[function_name]['start_time'], test_function[function_name]['end_time'], cold_start=False
            )
            save_to_csv(
                os.path.join(path_to_save_csv, f'warmStart_{function_name}_{layer_config.split(":")[-1]}.csv'),
                test_function[function_name]['query_results']
            )
        else:
            print(f"Warm start testing disabled for {function_name}")

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

def convert_csv_to_html(directory_path: str):
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    html_output_dir = os.path.join(directory_path, 'html_files')
    os.makedirs(html_output_dir, exist_ok=True)

    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        df = pd.read_csv(file_path)

        styled_df = df.style \
            .set_properties(**{'background-color': '#f0f8ff', 'border-color': '#4682b4', 'border-style': 'solid', 'border-width': '1px', 'text-align': 'center'})\
            .set_table_styles([{'selector': 'th', 'props': [('background-color', '#4682b4'), ('color', 'white'), ('font-weight', 'bold'), ('text-align', 'center')]}])\
            .highlight_max(subset=[col for col in df.columns if col not in ['FunctionName', 'Runtime', 'MemorySize', 'Timeout']], color='#90ee90')\
            .highlight_min(subset=[col for col in df.columns if col not in ['FunctionName', 'Runtime', 'MemorySize', 'Timeout']], color='#ffcccb')

        html_file_name = f"{os.path.splitext(csv_file)[0]}.html"
        html_path = os.path.join(html_output_dir, html_file_name)

        with open(html_path, 'w') as f:
            f.write('<html><head><style>table {border-collapse: collapse; margin: 20px;} body {font-family: Arial, sans-serif;}</style></head><body>')
            f.write(f'<h3>Results for {csv_file}</h3>')
            f.write(styled_df.to_html())
            f.write('</body></html>')

        print(f"Converted {csv_file} to {html_file_name}")

def run_parallel_invocations(path_to_save_csv, enable_cold_start=True, enable_warm_start=True, enable_prod_layer=True):
    if os.path.exists(path_to_save_csv):
        for file in os.listdir(path_to_save_csv):
            os.remove(os.path.join(path_to_save_csv, file))

    print(f"Testing configuration: Cold Start: {'Enabled' if enable_cold_start else 'Disabled'}, Warm Start: {'Enabled' if enable_warm_start else 'Disabled'}, Production Layer: {'Enabled' if enable_prod_layer else 'Disabled'}")

    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(invoke_lambda_function, fn_name, layers, path_to_save_csv, enable_cold_start, enable_warm_start, enable_prod_layer): fn_name
            for fn_name, layers in LAMBDA_FUNCTIONS_WITH_LAYERS.items()
        }
        for future in as_completed(futures):
            fn_name = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred with {fn_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Invoke AWS Lambda and optionally convert CSV to HTML.')
    parser.add_argument('--csv_path', type=str, default='./test-results', help='Directory path to save CSV.')
    parser.add_argument('--html', action='store_true', help='Convert CSV files to HTML')
    parser.add_argument('--disable-cold-start', action='store_true', help='Disable cold start testing')
    parser.add_argument('--disable-warm-start', action='store_true', help='Disable warm start testing')
    parser.add_argument('--disable-prod-layer', action='store_true', help='Disable production layer testing (skip fetching layer from API)')
    args = parser.parse_args()
    
    PATH_TO_SAVE_CSV = args.csv_path
    enable_cold_start = not args.disable_cold_start
    enable_warm_start = not args.disable_warm_start
    enable_prod_layer = not args.disable_prod_layer
    
    # Validate that at least one test type is enabled
    if not enable_cold_start and not enable_warm_start:
        print("Error: Both cold start and warm start testing cannot be disabled simultaneously.")
        exit(1)
    
    print("Starting parallel invocation of Lambda functions with layers...")
    run_parallel_invocations(PATH_TO_SAVE_CSV, enable_cold_start, enable_warm_start, enable_prod_layer)
    print("All Lambda functions have been invoked successfully.")

    if args.html:
        print(f"Converting CSV files in {PATH_TO_SAVE_CSV} to HTML...")
        convert_csv_to_html(PATH_TO_SAVE_CSV)
        print("CSV files have been converted to HTML.")