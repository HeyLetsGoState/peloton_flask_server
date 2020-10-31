from flask import Flask, jsonify
from flask_cors import CORS
from pytz import timezone

import datetime
import boto3

app = Flask(__name__)
app.config.from_object(__name__)

# Enable Cross-origin resource sharing since we have port 8080 for the UI and 5000 here
# Unless of course you choose to run templates and run it all out of here
CORS(app, resources={r'/*': {'origins': '*'}})

client = boto3.client('dynamodb')
eastern = timezone('US/Eastern')

"""
Just a health-check to make sure we're properly deployed
"""


@app.route("/ping", methods=['GET'])
def ping():
    return jsonify('pong!')


"""
We store all of our ride information in dynamo by the unique key of the epcoh
so yank that out, parse it to a date-time and then sort it and then return it
This gets us our labels for the x-axis going from oldest to newest
"""


@app.route("/get_labels", methods=['GET'])
def get_labels():
    items = client.scan(
        TableName="peloton_ride_data"
    )
    averages = items.get("Items")

    ride_times = [r.get("ride_Id") for r in averages]
    ride_times = [datetime.datetime.fromtimestamp(int(r.get('S')), tz=eastern).strftime('%Y-%m-%d') for r in ride_times]
    # Why doesn't sort return anything
    ride_times.sort()
    return jsonify(ride_times)


"""
Felt that grabbing the heart-rate info on it's own return was useful for the one-off Heart Rate Chart
"""


@app.route("/get_heart_rate", methods=['GET'])
def get_heart_rate():
    items = client.scan(
        TableName="peloton_ride_data"
    )

    # Grab my data
    data = items.get("Items")
    # Then sort it
    data = sorted(data, key=lambda i: i['ride_Id'].get('S'))

    heart_rate = [f.get('Avg Output').get('M').get('heart_rate').get('N') for f in data]
    heart_rate = [int(h) if h is not None else 0 for h in heart_rate]
    return jsonify(heart_rate)


"""
Generate the chart data for the average outputs of Output/Cadence/Resistance/Speed/Miles
"""


@app.route("/get_charts", methods=['GET'])
def get_charts():
    items = client.scan(
        TableName="peloton_ride_data"
    )

    averages = items.get("Items")
    averages = sorted(averages, key=lambda i: i['ride_Id'].get('S'))
    average_output = [f.get("Avg Output").get('M').get("value").get('N') for f in averages]
    average_cadence = [f.get("Avg Cadence").get('M').get("value").get('N') for f in averages]
    average_resistance = [f.get("Avg Resistance").get('M').get("value").get('N') for f in averages]
    average_speed = [f.get("Avg Speed").get('M').get("value").get('N') for f in averages]
    miles_per_ride = [f.get("Avg Output").get('M').get("miles_ridden").get('N') for f in averages]

    datasets = [average_output, average_cadence, average_resistance, average_speed, miles_per_ride]
    return jsonify(datasets)


"""
Pull back course data information to display in a table
"""


@app.route("/course_data")
def get_course_data():
    items = client.scan(
        TableName="peloton_course_data"
    )
    return_data = {}

    course_data = items.get("Items")
    course_data = sorted(course_data, key=lambda i: i['created_at'].get('S'))

    for course in course_data:
        return_data[course.get('created_at').get('S')] = {
            'name': course.get('name').get('S'),
            'difficulty': course.get('difficulty').get('S'),
            'length': course.get('length').get('S'),
            'instructor': course.get('instructor', {}).get('S'),
            'date': datetime.datetime.fromtimestamp((int(course.get('created_at', {}).get('S'))), tz=eastern).strftime(
                '%Y-%m-%d')
        }

    return jsonify(return_data)


if __name__ == "__main__":
    app.run(debug=True)
