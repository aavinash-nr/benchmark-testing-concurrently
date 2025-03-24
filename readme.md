### Installing the required modules

Change the directory to root module and then please run the below command.

```
pip install -r requirements.txt
```

### Running the code 
In the below give function arn and no of concurrent users and duration it need to be called it will infinitely call till that duration


#### Concurrent Testing: 
It wait for `10`  mins to fetch the data from cloud watch for the invocations which are happened using this code. 
```
python invoke_concurrently.py --function_arn arn:aws:lambda:region:account-id:function:function-name --concurrent_users 10 --duration 60
```
To save to CSV file 

```
python invoke_concurrently.py --function_arn arn:aws:lambda:region:account-id:function:function-name --concurrent_users 10 --duration 60 --output_file output.csv
```


### Measure Cold Start for multiple 
Just add the function names in the list, update region  and run the python file. It will run and create a dir called `test-results` and save into two different files for both `coldStart` and `warmStart` for all the functions given in the list.
```
python measureDuration.py
```



