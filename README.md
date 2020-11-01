## Peloton Flask Server
This web server is powered by Flask and is used to push and pull 
peloton information.  

* The class peloton_connection is used to push data to DynamoDB
* The main.py has the rest calls to pull the data out and generate
* To push the data up look at test.py (this can be retro-fitted to run on all data or set-up on a cron to do it)

Front-End Data can be populated with these rest calls and information can be 
display via ChartJS 


If you think this has helped you on your journey, I'd love a cup of coffee

<a href="https://www.buymeacoffee.com/psukardi"><img src="https://img.buymeacoffee.com/button-api/?text=Buy me a coffee&emoji=&slug=psukardi&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=ffffff"></a>
