import boto3
import json
from flask_cors import CORS
from datetime import datetime
from jproperties import Properties
from pytz import timezone
from connection.peloton_connection import PelotonConnection
from flask import Flask, jsonify, request, Response, session, redirect, make_response
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user

app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(SECRET_KEY="1234567")
conn = PelotonConnection()

# CORS Set-up here and at the bottom
CORS(app, resources={r'/*': {'origins': '*', 'allowedHeaders': ['Content-Type']}})
app.config['CORS_HEADERS'] = 'Content-Type'
client = boto3.client('dynamodb')
eastern = timezone('US/Eastern')

# flask-login
login_manager = LoginManager()
login_manager.init_app(app)
# Force the user to goto /login if they're not logged in
login_manager.login_view = "login"

p = Properties()
with open("peloton.properties", "rb") as f:
    p.load(f, "utf-8")

default_user_id = p["USER_ID"].data

'''
Create my User Model
'''


class User(UserMixin):
    def __init__(self, id):
        self.id = id
        self.name = "user" + id
        self.passwd = self.name + "_secret"

    def __repr__(self):
        return "%d/%s/%s/%s" % (self.id, self.name, self.password)


"""
Just a health-check to make sure we're properly deployed
"""


@app.route("/ping", methods=['GET'])
@login_required
def ping():
    return jsonify('pong!')


"""
We store all of our ride information in dynamo by the unique key of the epcoh
so yank that out, parse it to a date-time and then sort it and then return it
This gets us our labels for the x-axis going from oldest to newest
"""


@app.route("/get_labels/", methods=['GET'])
@app.route("/get_labels/<user_id>", methods=['GET'])
def get_labels(user_id=None):
    # I'll  use this as my model for forcing logins in the future
    # current_user = flask_login.current_user.id
    #
    # if current_user == 'guest':
    #     random_data = list(range(50,85))
    #     random.shuffle(random_data)
    #     return jsonify(random_data)

    items = client.scan(
        TableName="peloton_ride_data"
    )
    averages = items.get("Items")

    user_id = user_id if user_id is not None else default_user_id
    ride_times = [r.get("ride_Id") for r in averages if r.get('user_id').get('S') == user_id]
    ride_times = [datetime.fromtimestamp(int(r.get('S')), tz=eastern).strftime('%Y-%m-%d') for r in ride_times]
    # Why doesn't sort return anything
    ride_times.sort()
    return jsonify(ride_times)


"""
Felt that grabbing the heart-rate info on it's own return was useful for the one-off Heart Rate Chart
"""


@app.route("/get_heart_rate/", methods=['GET'])
@app.route("/get_heart_rate/<user_id>", methods=['GET'])
def get_heart_rate(user_id=None):
    items = client.scan(
        TableName="peloton_ride_data"
    )

    user_id = user_id if user_id is not None else default_user_id
    # Grab my data
    data = items.get("Items")
    # Then sort it
    data = [d for d in data if d.get('user_id').get('S') == user_id]
    data = sorted(data, key=lambda i: i['ride_Id'].get('S'))

    heart_rate = [f.get('Avg Output').get('M').get('heart_rate').get('N') for f in data]
    heart_rate = [int(h) if h is not None else 0 for h in heart_rate]
    return jsonify(heart_rate)


"""
Generate the chart data for the average outputs of Output/Cadence/Resistance/Speed/Miles
"""


@app.route("/get_charts/", methods=['GET'])
@app.route("/get_charts/<user_id>", methods=['GET'])
def get_charts(user_id=None):
    items = client.scan(
        TableName="peloton_ride_data"
    )

    averages = items.get("Items")

    user_id = user_id if user_id is not None else default_user_id
    # Trim this down to just ME
    averages = [a for a in averages if a.get('user_id').get('S') == user_id]
    averages = sorted(averages, key=lambda i: i['ride_Id'].get('S'))
    average_output = [f.get("Avg Output").get('M').get("value").get('N') for f in averages]
    average_cadence = [f.get("Avg Cadence").get('M').get("value").get('N') for f in averages]
    average_resistance = [f.get("Avg Resistance").get('M').get("value").get('N') for f in averages]
    average_speed = [f.get("Avg Speed").get('M').get("value").get('N') for f in averages]
    miles_per_ride = [f.get("Avg Output").get('M').get("miles_ridden").get('N') for f in averages]

    datasets = [average_output, average_cadence, average_resistance, average_speed, miles_per_ride]
    return jsonify(datasets)


@app.route("/peloton_login", methods=['POST'])
def peloton_login():
    creds = request.get_json()
    data = {
        "username_or_email": f"{creds.get('email')}",
        "password": f"{creds.get('passwd')}"
    }

    auth_response = conn.post("https://api.onepeloton.com/auth/login", json.dumps(data))
    session_id = auth_response.get("session_id")
    user_id = auth_response.get("user_id")
    cookies = dict(peloton_session_id=session_id)

    return {
        'user_id': user_id,
        'cookies': cookies
    }


@app.route("/get_user_rollup", methods=['GET'])
def get_user_rollup():
    credentials = request.get_json()
    cookies = json.loads(request.args.get('cookies'))
    user_id = request.args.get('user_id')

    items = client.scan(
        TableName="peloton_ride_data"
    )

    averages = items.get("Items")
    averages = sorted(averages, key=lambda i: i['ride_Id'].get('S'))

    miles_ridden = sum([float(r.get('Avg Cadence').get('M').get('miles_ridden').get('N')) for r in averages])
    total_achievements = averages[-1].get('total_achievements').get('N')
    user_info = conn.get_user_info(user_id, cookies)

    return jsonify({
        'total_miles': miles_ridden,
        'total_rides': user_info.get('total_pedaling_metric_workouts'),
        'total_achievements': total_achievements,
        'photo_url': user_info.get('image_url'),
        'name': f"{user_info.get('first_name')} {user_info.get('last_name')}"
    })


"""
Pull back course data information to display in a table
"""


@app.route("/course_data/")
@app.route("/course_data/<user_id>")
def get_course_data(user_id=None):
    items = client.scan(
        TableName="peloton_course_data"
    )
    return_data = {}

    course_data = items.get("Items")

    user_id = user_id if user_id is not None else default_user_id
    course_data = [c for c in course_data if c.get('user_id').get('S') == user_id]
    course_data = sorted(course_data, key=lambda i: i['created_at'].get('S'))

    for course in course_data:
        return_data[course.get('created_at').get('S')] = {
            'name': course.get('name').get('S'),
            'difficulty': course.get('difficulty').get('S'),
            'length': course.get('length').get('S'),
            'instructor': course.get('instructor', {}).get('S'),
            'date': datetime.fromtimestamp((int(course.get('created_at', {}).get('S'))), tz=eastern).strftime(
                '%Y-%m-%d')
        }

    return jsonify(return_data)


@app.route("/music_by_time/<ride_time>")
def get_music_by_time(ride_time=None):
    items = client.scan(
        TableName="peloton_music_sets"
    )

    # TODO - Get a utility class to dump these S's and L's' and the rest from Dynamo
    music = [i for i in items.get("Items") if i.get('created_at').get('S') == ride_time]
    if music is not None:
        music_set = [song.get('S') for song in music[0].get('set_list').get('L')]
    return jsonify(music_set)


# somewhere to login
@app.route("/login", methods=["GET", "POST"])
def login():
    # Force a recycle
    logout_user()
    session.clear()
    session.modified = True
    if request.method == 'POST':
        username = request.form['username']
        psw = request.form['password']

        data = {
            'username_or_email': username,
            'password': psw
        }

        # Log into Peloton
        data = json.dumps(data)
        auth_response = conn.post("https://api.onepeloton.com/auth/login", data)

        # Create the cookie, yank the user ID and the session ID
        session_id = auth_response.get("session_id")
        user_id = auth_response.get("user_id")
        cookies = dict(peloton_session_id=session_id)

        response = make_response(redirect("http://pelodashboard.com"))
        response.set_cookie('USER_ID', session['USER_ID'])
        return response

    else:
        return Response('''
                <h3>Peloton Login</h3>
                <p>Please enter your credentials to pull the analytic data.  No credentials will be stored and
                will simply be passed through to the provider for authorization</p>
                <form action="" method="post">
                    <p><input type=text name=username>
                    <p><input type=password name=password>
                    <p><input type=submit value=Login>
                </form>
                ''')


# somewhere to logout
@app.route("/logout")
@login_required
def logout():
    logout_user()
    session.clear()
    session.modified = True
    return redirect('http://pelodashboard.com/')


# handle login failed
@app.errorhandler(401)
def page_not_found(e):
    return Response('<p>Login failed</p>')


# callback to reload the user object
@login_manager.user_loader
def load_user(userid):
    return User(userid)


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', 'http://pelodashboard.com')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response


if __name__ == "__main__":
    app.run(debug=True)