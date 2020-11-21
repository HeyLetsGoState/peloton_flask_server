import boto3
import flask_login
import json
import asyncio
from itertools import chain
import numpy
from boto3.dynamodb.conditions import Key
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
dynamodb = boto3.resource('dynamodb')

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

async def pull_user_data_async(user_id, cookies):
    conn.get_most_recent_ride_details(user_id, cookies, True)
    conn.get_most_recent_ride_info(user_id, cookies, True)

    if user_id is None:
        raise InvalidUsage('Your peloton credentials were invalid.  Please verify and try again', status_code=401)

    response = make_response(redirect("http://pelodashboard.com"))
    response.set_cookie('USER_ID', user_id)

    __update_user_data(user_id)
    __delete_keys__(user_id=user_id)
    return response

user_pull = asyncio.get_event_loop()


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
    return user_pull.run_until_complete(pull_user_data_async(user_id, cookies))


@app.route("/ride_graph/history/<user_id>/<ride_id>")
@cache.cached(timeout=3600, query_string=True)
def get_ride_history(user_id=None, ride_id=None):
    return jsonify(conn.get_ride_history(user_id, ride_id))


@app.route("/ride_graph/<ride_hash>")
@cache.cached(timeout=3600, query_string=True)
def get_ride_graph(ride_hash=None):
    if ride_hash == 0:
        return jsonify({})
    else:
        table = dynamodb.Table('peloton_graph_data')
        response = table.query(
            KeyConditionExpression=Key('workout_hash').eq(ride_hash)
        )

        try:
            my_ride = response['Items'][0]
        except Exception as e:
            print(e)
            return jsonify({})

        return_obj = {
            'output': my_ride.get('metrics').get('Output'),
            'cadence': my_ride.get('metrics').get('Cadence'),
            'resistance': my_ride.get('metrics').get('Resistance'),
            'speed': my_ride.get('metrics').get('Speed'),
            'totals': {
                'calories': my_ride.get('Calories'),
                'distance': my_ride.get('Distance'),
                'total_output': my_ride.get('Total Output')
            },
            'seconds_since_start': my_ride.get('seconds_since_pedaling_start')
        }

        return jsonify(return_obj)


@app.route("/get_labels/<user_id>")
@cache.cached(timeout=3600, query_string=True)
def get_labels(user_id=None):
    if user_id is None:
        user_id = default_user_id

    ride_data = __get_user_labels__(user_id)

    ride_times = [r.get("ride_Id") for r in ride_data]
    ride_times = [datetime.fromtimestamp(int(r), tz=eastern).strftime('%Y-%m-%d') for r in ride_times]
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
    return jsonify(conn.get_achievements(user_id))


@app.route("/get_user_rollup/<user_id>", methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_user_rollup(user_id=None):
    averages = dump_table('peloton_ride_data')
    averages = [a for a in averages if a.get('user_id').get('S') == user_id]
    averages = sorted(averages, key=lambda i: i['ride_Id'].get('S'))
    total_rides = len(averages)
    miles_ridden = sum([float(r.get('Avg Cadence').get('M', {}).get('miles_ridden',{}).get('N', 0)) for r in averages])
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

    # I'll fix this with some comprehension later
    hash_id_combo = {}
    for workout in user_workouts:
        peloton_id = workout.get('peloton_id')
        if peloton_id not in hash_id_combo:
            hash_id_combo[peloton_id] = []
        hash_id_combo[peloton_id].append(workout.get('workout_hash'))


    workout_hash = [w.get('workout_hash') for w in user_workouts]

    if workout_hash is None or len(workout_hash) == 0:
        raise InvalidUsage('Your Peloton Data is missing.  '
                           'Please try re-loading your data from the home page. Please try again', status_code=204)

    # Cross reference against the course data to bring back minimal record set
    batch_keys = {
        "peloton_course_data": {
            'Keys': [{'workout_hash': user_hash} for user_hash in workout_hash]
        }
    }

    total_responses = []

    split_data = numpy.array_split(batch_keys.get('peloton_course_data').get('Keys'), 20)
    for split in split_data:
        batch_key = {
            'peloton_course_data': {
                 'Keys': split.tolist()
            }
        }
        response = dynamodb.batch_get_item(RequestItems=batch_key)
        response = [c for c in response.get('Responses').get('peloton_course_data')]
        total_responses.append(response)

    response = list(chain.from_iterable(total_responses))
    response = sorted(response, key=lambda i: i['created_at'])

    courses_with_duplicates = [h[1] for h in hash_id_combo.items() if len(h[1]) > 1]

    for course in response:
        return_data[course.get('created_at')] = {
            'name': course.get('name'),
            'difficulty': course.get('difficulty'),
            'length': course.get('length'),
            'instructor': course.get('instructor', {}),
            'date': datetime.fromtimestamp((int(course.get('created_at', {}))), tz=eastern).strftime(
                '%Y-%m-%d'),
            'workout_hash': course.get('workout_hash'),
            'multiple_rides': course.get('workout_hash') in courses_with_duplicates[0] # I need to fix with comp

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

    table = dynamodb.Table('peloton_ride_data')
    response = table.query(
        IndexName="user_id-index",
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    return response['Items']


def __get_user_labels__(user_id):
    table = dynamodb.Table('peloton_ride_data')
    response = table.query(
        IndexName="user_id-index",
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    return response['Items']


def __update_user_data(user_id=None):
    table = dynamodb.Table('peloton_user')
    rider_info = table.query(
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    ride_item = {
        'user_id' : user_id,
        'ride_list': rider_info['Items'][0].get('ride_list')
    }

    ddb_data = json.loads(json.dumps(ride_item))
    table.put_item(Item=ddb_data)


def __delete_keys__(user_id: str):
    with app.app_context():
        cache.clear()
    """
    # This should speed up the caching a bit and let this thing scale a bit easier.
    # One day I'll quit being cheap and move off the t2.micro
    # :param user_id:  the person to clear out
    # :return:
    # """
    #
    # if user_id is None:
    #     return
    #
    # pattern = f"*{user_id}*"
    # cache.delete_memoized('/get_user_rollup/', user_id)
    # cache.delete_memoized('/course_data/', user_id)
    # cache.delete_memoized('/get_user_rollup/', user_id)
    # cache.delete_memoized('/get_charts/', user_id)
    # cache.delete_memoized('/get_heart_rate', user_id)
    # cache.delete_memoized('/get_ride_charts' , user_id)


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
