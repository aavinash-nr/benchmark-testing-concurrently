### Installing the required modules

Change the directory to root module and then please run the below command.

```
pip install -r requirements.txt
```

### Running the code 

#### Concurrent Testing: 
It wait for `10`  mins to fetch the data from cloud watch for the invocations which are happened using this code. 
```
python invoke_concurrently.py --function_arn arn:aws:lambda:region:account-id:function:function-name --concurrent_users 100 --duration 10
```
To save to CSV file 

```
python invoke_concurrently.py --function_arn arn:aws:lambda:region:account-id:function:function-name --concurrent_users 10 --duration 60 --output_file output.csv
```

### Measure Cold Start and Warm Start for Multiple Lambda Functions

The `measureNew.py` script provides comprehensive testing of Lambda functions with different layer configurations. It supports both cold start and warm start testing with flexible options.

#### Configuration
1. Update the `LAMBDA_FUNCTIONS_WITH_LAYERS` dictionary with your function names and layer ARNs
2. Set the `REGION` variable to your desired AWS region
3. Adjust `MAX_INVOCATIONS`, `SLEEP_TIME_FOR_INVOCATION`, and `WAIT_TIME_BETWEEN_PHASES` as needed

#### Basic Usage Examples

```bash
# Run both cold start and warm start tests with production layers (default)
python measureNew.py

# Specify custom CSV output directory
python measureNew.py --csv_path ./my-test-results

# Generate HTML reports from CSV files
python measureNew.py --html
```

#### Selective Testing Options

```bash
# Run only cold start tests
python measureNew.py --disable-warm-start

# Run only warm start tests
python measureNew.py --disable-cold-start

# Skip production layer testing (only test predefined layers)
python measureNew.py --disable-prod-layer

# Combine options: only cold start tests without production layer
python measureNew.py --disable-warm-start --disable-prod-layer 

# Test with custom path and HTML generation, no production layer
python measureNew.py --csv_path ./results --html --disable-prod-layer
```

#### Output
- Creates CSV files for each function and test type: `coldStart_{function_name}_{layer_version}.csv` and `warmStart_{function_name}_{layer_version}.csv`
- Optional HTML reports with styled tables for better visualization
- Comprehensive metrics including percentiles, averages, and init duration statistics

#### Command Line Options
- `--csv_path`: Directory path to save CSV files (default: `./test-results`)
- `--html`: Convert CSV files to HTML with styling
- `--disable-cold-start`: Skip cold start testing
- `--disable-warm-start`: Skip warm start testing  
- `--disable-prod-layer`: Skip fetching and testing production layers from API

The script automatically fetches the latest production layer for each runtime from the New Relic layers API and tests it alongside your predefined layers.



