import boto3
import flask_login
import json
from connection.invalid_usage import InvalidUsage
from jproperties import Properties
from flask_cors import CORS
from datetime import datetime
from pytz import timezone
from connection.peloton_connection import PelotonConnection
from flask import Flask, jsonify, request, Response, session, redirect, make_response
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user
from flask_caching import Cache


app = Flask(__name__)
app.config.from_object(__name__)
app.config.update(SECRET_KEY="1234567")
conn = PelotonConnection()
try:
    """
    In a local environment you can't use redis (well you could by why would you)
    And for now I won't either until I can figure out the key issue.
    """
    cache = Cache(config={'CACHE_TYPE': 'simple'})
except Exception:
    cache = Cache(config={'CACHE_TYPE': 'simple'})

cache.init_app(app)

# CORS Set-up here and at the bottom
CORS(app, resources={r'/*': {'origins': '*', 'allowedHeaders': ['Content-Type']}})
app.config['CORS_HEADERS'] = 'Content-Type'
client = boto3.client('dynamodb')
eastern = timezone('US/Eastern')

# flask-login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


'''
Create my User Model
'''

p = Properties()
with open("peloton.properties", "rb") as f:
    p.load(f, "utf-8")

default_user_id = p["USER_ID"].data


class User(UserMixin):
    def __init__(self, id):
        self.id = id
        self.name = id
        self.passwd = self.name + "_secret"

    def __repr__(self):
        return "%d/%s/%s/%s" % (self.id, self.name, self.password)


@app.route("/ping", methods=['GET'])
@login_required
def ping():
    """
    Just a health-check to make sure we're properly deployed
    """
    return jsonify('pong!')


@app.route('/get_total_users', methods=['GET'])
def get_user_count():
    total_users = dump_table('peloton_user')
    resp_obj = {
        'total_users' : len(total_users)
    }
    return jsonify(resp_obj)


@app.route("/pull_user_data", methods=['GET'])
@login_required
def pull_user_data():
    """
    We store all of our ride information in dynamo by the unique key of the epcoh
    so yank that out, parse it to a date-time and then sort it and then return it
    This gets us our labels for the x-axis going from oldest to newest
    """
    # Run this daily or set-up a cron to do it for you
    user_id = session.get('USER_ID', None)
    cookies = session['COOKIES']
    conn.get_most_recent_ride_details(user_id, cookies, True)
    conn.get_most_recent_ride_info(user_id, cookies, True)

    if user_id is None:
        raise InvalidUsage('Your peloton credentials were invalid.  Please verify and try again', status_code=401)

    response = make_response(redirect("http://pelodashboard.com"))
    response.set_cookie('USER_ID', user_id)

    __update_user_data()
    __delete_keys__(user_id=user_id)
    return response


@app.route("/ride_graph/<ride_hash>")
@cache.cached(timeout=3600, query_string=True)
def get_ride_graph(ride_hash=None):
    if ride_hash == 0:
        return jsonify({})
    else:
        rides = dump_table('peloton_graph_data')

        my_ride = [r for r in rides if r.get('workout_hash').get('S') == ride_hash][0]

    return_obj = {
        'output': [o.get('N') for o in my_ride.get('metrics').get('M').get('Output').get('L')],
        'cadence': [o.get('N') for o in my_ride.get('metrics').get('M').get('Cadence').get('L')],
        'resistance': [r.get('N') for r in my_ride.get('metrics').get('M').get('Resistance').get('L')],
        'speed': [r.get('N') for r in my_ride.get('metrics').get('M').get('Speed').get('L')],
        'totals': {
            'calories': my_ride.get('summaries').get('M').get('Calories').get('N'),
            'distance': my_ride.get('summaries').get('M').get('Distance').get('N'),
            'total_output': my_ride.get('summaries').get('M').get('Total Output').get('N'),
        },
        'seconds_since_start': [s.get('N') for s in my_ride.get('seconds_since_pedaling_start').get('L')]
    }

    return jsonify(return_obj)


@app.route("/get_labels/<user_id>")
@cache.cached(timeout=3600, query_string=True)
def get_labels(user_id=None):

    averages = dump_table('peloton_ride_data')
    peloton_id = user_id if user_id is not None else default_user_id

    ride_times = [r.get("ride_Id") for r in averages if r.get('user_id').get('S') == peloton_id]
    ride_times = [datetime.fromtimestamp(int(r.get('S')), tz=eastern).strftime('%Y-%m-%d') for r in ride_times]
    # Why doesn't sort return anything
    ride_times.sort()
    return jsonify(ride_times)


@app.route("/get_ride_charts/<user_id>")
@cache.cached(timeout=3600, query_string=True)
def get_ride_charts(user_id=None):

    averages = dump_table('peloton_ride_data')
    peloton_id = user_id if user_id is not None else default_user_id

    rides_with_hash = [(r.get('ride_Id').get('S'), r.get('workout_hash').get('S')) for r in averages if r.get('user_id').get('S') == peloton_id]
    rides_with_hash = [((datetime.fromtimestamp(int(r[0]), tz=eastern).strftime('%Y-%m-%d')), r[1]) for r in rides_with_hash]

    """
    Why doesn't sort return anything?  Because it doesn't feel like it 
    """
    rides_with_hash.sort()
    return jsonify(rides_with_hash)


@app.route("/get_heart_rate/<user_id>", methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_heart_rate(user_id=None):
    """
    Felt that grabbing the heart-rate info on it's own return was useful for the one-off Heart Rate Chart
    """
    peloton_id = user_id if user_id is not None else default_user_id

    # Grab and sort data
    data = dump_table('peloton_ride_data')
    data = [d for d in data if d.get('user_id').get('S') == peloton_id]
    data = sorted(data, key=lambda i: i['ride_Id'].get('S'))

    heart_rate = [f.get('Avg Output', {}).get('M', {}).get('heart_rate', {}).get('N', 0) for f in data]
    heart_rate = [int(h) if h is not None else 0 for h in heart_rate]
    return jsonify(heart_rate)



@app.route("/get_charts/<user_id>", methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_charts(user_id=None):
    """
    Generate the chart data for the average outputs of Output/Cadence/Resistance/Speed/Miles
    """
    peloton_id = user_id if user_id is not None else default_user_id

    averages = dump_table('peloton_ride_data')
    averages = [a for a in averages if a.get('user_id').get('S') == peloton_id]

    averages = sorted(averages, key=lambda i: i['ride_Id'].get('S'))
    average_output = [f.get("Avg Output", {}).get('M', {}).get("value", {}).get('N', 0) for f in averages]
    average_cadence = [f.get("Avg Cadence", {}).get('M', {}).get("value", {}).get('N', 0) for f in averages]
    average_resistance = [f.get("Avg Resistance", {}).get('M', {}).get("value", {}).get('N', 0) for f in averages]
    average_speed = [f.get("Avg Speed", {}).get('M', {}).get("value", {}).get('N', 0) for f in averages]
    miles_per_ride = [f.get("Avg Output", {}).get('M', {}).get("miles_ridden", {}).get('N', 0) for f in averages]

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


@app.route("/achievements/<user_id>", methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_achievements(user_id=None):
    user_id = session.get('USER_ID', None)
    cookies = session['COOKIES']

    return jsonify(conn.get_achievements(user_id, cookies))


@app.route("/get_user_rollup/<user_id>", methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_user_rollup(user_id=None):
    averages = dump_table('peloton_ride_data')
    averages = [a for a in averages if a.get('user_id').get('S') == user_id]
    averages = sorted(averages, key=lambda i: i['ride_Id'].get('S'))
    total_rides = len(averages)
    miles_ridden = sum([float(r.get('Avg Cadence').get('M').get('miles_ridden').get('N', 0)) for r in averages])
    total_achievements = None
    try:
        total_achievements = averages[-1].get('total_achievements').get('N')
    except Exception:
        total_achievements: "0"

    return jsonify({
        'total_miles': miles_ridden,
        'total_rides': total_rides,
        'total_achievements': total_achievements
    })


@app.route("/course_data/<user_id>")
@cache.cached(timeout=3600, query_string=True)
def get_course_data(user_id=None):
    """
    Pull back course data information to display in a table
    """
    dynamodb = boto3.resource('dynamodb')
    return_data = {}

    # Get all the workout hashes for the given user
    user_workouts = __get_user_workouts__(user_id)
    if user_workouts.get('Item') is None:
        raise InvalidUsage('Your Peloton Data is missing.  '
                           'Please try re-loading your data from the home page. Please try again', status_code=204)

    ride_list = [r.get('S') for r in user_workouts['Item'].get('ride_list').get('L')]

    # Cross reference against the course data to bring back minimal record set
    peloton_ride_data_table = dynamodb.Table('peloton_course_data')
    batch_keys = {
        peloton_ride_data_table.name: {
            'Keys': [{'workout_hash': user_hash} for user_hash in ride_list]
        }
    }

    # Bring back the data && sort it
    response = dynamodb.batch_get_item(RequestItems=batch_keys)
    response = [c for c in response.get('Responses').get('peloton_course_data')]
    response = sorted(response, key=lambda i: i['created_at'])
    # course_data = sorted(course_data, key=lambda i: i['created_at'].get('S'))

    for course in response:
        return_data[course.get('created_at')] = {
            'name': course.get('name'),
            'difficulty': course.get('difficulty'),
            'length': course.get('length'),
            'instructor': course.get('instructor', {}),
            'date': datetime.fromtimestamp((int(course.get('created_at', {}))), tz=eastern).strftime(
                '%Y-%m-%d'),
            'workout_hash': course.get('workout_hash')
        }

    return jsonify(return_data)


@app.route("/music_by_time/<ride_time>")
@cache.cached(timeout=3600, query_string=True)
def get_music_by_time(ride_time=None):

    music = dump_table('peloton_music_sets')

    # TODO - Get a utility class to dump these S's and L's' and the rest from Dynamo
    music = [i for i in music if i.get('created_at').get('S') == ride_time]
    if music is not None:
        music_set = [song.get('S', None) for song in music[0].get('set_list', {}).get('L', {})]
    return jsonify(music_set)


# somewhere to login
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == 'POST':
        username = request.form['username']
        psw = request.form['password']

        user = User(username)
        login_user(user)
        current_user = load_user(flask_login.current_user.id)

        data = {
            'username_or_email': current_user.name,
            'password': psw
        }

        # Log into Peloton
        data = json.dumps(data)
        auth_response = conn.post("https://api.onepeloton.com/auth/login", data)

        # Create the cookie, yank the user ID and the session ID
        session_id = auth_response.get("session_id")
        user_id = auth_response.get("user_id", None)
        cookies = dict(peloton_session_id=session_id)
        # now that they're logged in
        session['SESSION_ID'] = session_id
        session['USER_ID'] = user_id
        session['COOKIES'] = cookies

        if user_id is None:
            raise InvalidUsage('Your Peloton Credentials were invalid.  Please try again', status_code=401)

        response = make_response(redirect("http://pelodashboard.com"))
        response.set_cookie('USER_ID', user_id)
        return response
    else:
        return Response('''
<link href="//maxcdn.bootstrapcdn.com/bootstrap/4.1.1/css/bootstrap.min.css" rel="stylesheet" id="bootstrap-css">
<script src="//maxcdn.bootstrapcdn.com/bootstrap/4.1.1/js/bootstrap.min.js"></script>
<script src="//cdnjs.cloudflare.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
<!------ Include the above in your HEAD tag ---------->

<!doctype html>
<html lang="en">
<head>
    <!-- Required meta tags -->
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">

    <!-- Fonts -->
    <link rel="dns-prefetch" href="https://fonts.gstatic.com">
    <link href="https://fonts.googleapis.com/css?family=Raleway:300,400,600" rel="stylesheet" type="text/css">

    <link rel="stylesheet" href="css/style.css">

    <link rel="icon" href="Favicon.png">

    <!-- Bootstrap CSS -->
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css">

    <title>PeloDashboard Sign-In</title>
</head>
<body>

<main class="login-form">
    <div class="cotainer">
        <div class="row justify-content-center">
            <div class="col-md-8">
                <div class="card">
                    <div class="card-body">
                        <form action="" method="post"">
                            <div class="form-group row">
                                <label for="email_address" class="col-md-4 col-form-label text-md-right">Username or E-Mail Address</label>
                                <div class="col-md-6">
                                    <input type="text" class="form-control" name="username" required autofocus>
                                </div>
                            </div>

                            <div class="form-group row">
                                <label for="password" class="col-md-4 col-form-label text-md-right">Password</label>
                                <div class="col-md-6">
                                    <input type="password" id="password" class="form-control" name="password" required>
                                </div>
                            </div>

                            <div class="form-group row">
                                <div class="col-md-6 offset-md-4">
                                    <div class="checkbox">
                                        <label>
                                            <input type="checkbox" name="remember"> Remember Me
                                        </label>
                                    </div>
                                </div>
                            </div>

                            <div class="col-md-6 offset-md-4">
                                <button type="submit" class="btn btn-primary">
                                    Sign-In
                                </button>
                            </div>
                    </div>
                    </form>
                </div>
            </div>
        </div>
    </div>
    </div>

</main>







</body>
</html>
        ''')


# somewhere to logout
@app.route("/logout")
@login_required
def logout():
    logout_user()
    return Response('<p>Logged out</p>')


@app.route('/totals', methods=['GET'])
@cache.cached(timeout=60, query_string=True)
def get_total_rides():
    total_rides = dump_table('peloton_ride_data')
    total_users = dump_table('peloton_user')

    resp_obj = {
        'total_rides': len(total_rides),
        'total_users': len(total_users),
        'total_miles': sum([int(float(r.get('miles_ridden').get('N', 0))) for r in total_rides])
    }
    return jsonify(resp_obj)


# handle login failed
@app.errorhandler(401)
def page_not_found(e):
    return Response('<p>Login failed</p>')


# callback to reload the user object
@login_manager.user_loader
def load_user(userid):
    return User(userid)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def dump_table(table_name):
    results = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = client.scan(
                TableName=table_name,
                ExclusiveStartKey=last_evaluated_key
            )
        else:
            response = client.scan(TableName=table_name)
        last_evaluated_key = response.get('LastEvaluatedKey')

        results.extend(response['Items'])

        if not last_evaluated_key:
            break
    return results


def __get_user_workouts__(user_id):
    response = client.get_item(
        TableName="peloton_user",
        Key={
            'user_id': {'S': user_id}
        }
    )
    return response


def __update_user_data():
    riders = dump_table("peloton_ride_data")
    distinct_riders = set([r.get('user_id').get('S') for r in riders])

    # Now lets build out that user table
    table = boto3.resource('dynamodb').Table('peloton_user')

    for rider in distinct_riders:
        rider_info = [r for r in riders if r.get('user_id').get('S') == rider]
        workout_ids = [r.get('workout_hash').get('S') for r in rider_info]
        ride_item = {
            'user_id': rider,
            'ride_list': workout_ids
        }
        ddb_data = json.loads(json.dumps(ride_item))
        table.put_item(Item=ddb_data)


def __delete_keys__(user_id: str):
    """
    This should speed up the caching a bit and let this thing scale a bit easier.
    One day I'll quit being cheap and move off the t2.micro
    :param user_id:  the person to clear out
    :return:
    """

    if user_id is None:
        return

    pattern = f"*{user_id}*"
    cache.delete_memoized('/get_user_rollup/', user_id)
    cache.delete_memoized('/course_data/', user_id)
    cache.delete_memoized('/get_user_rollup/', user_id)
    cache.delete_memoized('/get_charts/', user_id)
    cache.delete_memoized('/get_heart_rate', user_id)
    cache.delete_memoized('/get_ride_charts' , user_id)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
