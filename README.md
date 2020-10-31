## Peloton Flask Server
This web server is powered by Flask and is used to push and pull 
peloton information.  

* The class peloton_connection is used to push data to DynamoDB
* The main.py has the rest calls to pull the data out and generate
* To push the data up look at test.py (this can be retro-fitted to run on all data or set-up on a cron to do it)

Front-End Data can be populated with these rest calls and information can be 
display via ChartJS 
