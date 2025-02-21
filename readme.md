### Installing the required modules

Change the directory to root module and then please run the below command.

```
pip install -r requirements.txt
```

### Running the code 
In the below give function arn and no of concurrent users and duration it need to be called it will infinitely call till that duration
```
python invoke_concurrently.py --function_arn arn:aws:lambda:region:account-id:function:function-name --concurrent_users 10 --duration 60
```
To save to CSV file 

```
python invoke_concurrently.py --function_arn arn:aws:lambda:region:account-id:function:function-name --concurrent_users 10 --duration 60 --output_file output.csv
```
