import boto3
from boto3.dynamodb.conditions import Key
import hashlib
import itertools
import json
import requests
import time
from connection.invalid_usage import InvalidUsage
from decimal import *

client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')


class PelotonConnection:
    HEADERS = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'peloton-platform': 'web'
    }

    def post(self, address, data=None):
        if data is not None:
            response = requests.post(address, headers=self.HEADERS, data=data)
        else:
            response = requests.post(address, headers=self.HEADERS)
        return response.json()

    def get(self, address, cookies):
        return requests.get(address, headers=self.HEADERS, cookies=cookies).json()

    def __get_achievements__(self, user_id, cookies):
        """
        One day I'll get better at nested comprehensions but for now this the best I can do
        At least it's not a super long method in JAVA
        :param self:
        :param user_id:
        :param cookies:
        :return:
        """
        achievements_url = f"https://api.onepeloton.com/api/user/{user_id}/achievements"
        achievements_details = self.get(achievements_url, cookies)
        achievements = [f for f in [a.get('achievements') for a in achievements_details.get('categories')]]
        achievements = [t.get('template') for t in [f for f in list(itertools.chain.from_iterable(achievements)) if f.get('count') > 0]]

        dict = []
        for achievement in achievements:
            dict.append({
                'name': achievement.get('name'),
                'image_url': achievement.get('image_url'),
                'description': achievement.get('description')
            })

        return dict

    @staticmethod
    def __get_workouts__(self, user_id, cookies):
        # Get my workout information
        page = 0
        my_workouts_url = f"https://api.onepeloton.com/api/user/{user_id}/workouts?page={page}"
        workout_results = []

        my_workouts = self.get(my_workouts_url, cookies)
        workout_results.append(my_workouts)

        while my_workouts.get('show_next') is True:
            page += 1
            my_workouts_url = f"https://api.onepeloton.com/api/user/{user_id}/workouts?page={page}"
            my_workouts = self.get(my_workouts_url, cookies)
            workout_results.append(my_workouts)

        final_results = list(itertools.chain([w.get('data') for w in workout_results]))
        try:
            workout_results = [y for x in final_results for y in x if x is not None]
            workout_results = [w for w in workout_results if w.get('fitness_discipline') == 'cycling'
                               or w.get('metrics_type') == 'cycling']

            workout_ids = [workout_id.get("id") for workout_id in workout_results]
            return workout_ids
        except Exception:
            raise InvalidUsage('There was an issue pulling your workouts, please try again later', status_code=401)

    @staticmethod
    def __get_user__(self, user_id, cookies):
        my_url = f"https://api.onepeloton.com/api/me"
        my_info = self.get(my_url, cookies)
        return my_info

    '''
    If you've never run this before, you can just remove the [0] and make this a for loop
    and iterate over each one
    '''

    def get_most_recent_ride_details(self, user_id=None, cookies=None, save=False):
        # Get the most recent workout ID
        workout_ids = PelotonConnection.__get_workouts__(self, user_id, cookies)

        """
        TODO: To clear some tech debt start adding the ride_id to the previous entries or find a way to 
        better do this.  I can't be itterating hundreds of rides
        """
        rides = None

        table = dynamodb.Table('peloton_ride_data')
        response = table.query(
            IndexName="user_id-index",
            KeyConditionExpression=Key('user_id').eq(user_id)
        )

        graphs = [g.get('workout_hash') for g in response['Items']]
        ride_ids = [g.get('ride_Id') for g in response['Items']]

        for workout_id in workout_ids:

            workout_url = f"https://api.onepeloton.com/api/workout/{workout_id}"
            # Get the workout info
            workout = self.get(workout_url, cookies)
            created_at = workout.get("created_at")
            if str(created_at) in ride_ids:
                continue

            d = {
                'created_at': created_at,
                'workout_id': workout.get('id'),
                'bike_id': workout.get('peloton_id')
            }


            """
            Now that more than one user wants to use this thing, we need to make each record super unique
            So we'll take the created at and the workout id and make that the hash.
            We'll combine the time of the ride, the id of the ride and the id of the bike
            """

            dhash = hashlib.md5()
            encoded = json.dumps(d, sort_keys=True).encode()
            dhash.update(encoded)
            workout_hash = dhash.hexdigest()

            performance_graph_url = f"https://api.onepeloton.com/api/workout/{workout_id}/performance_graph?every_n=5"
            graph = self.get(performance_graph_url, cookies)

            if workout_hash in graphs:
                continue

            if workout_hash not in graphs:
                graph_data = {
                    'workout_hash': str(workout_hash),
                    'averages': dict([(f.get('display_name') , f.get('value')) for f in graph.get('average_summaries')]),
                    'summaries': dict([(f.get('display_name'), f.get('value')) for f in graph.get('summaries')]),
                    'metrics': dict([(f.get('display_name'), f.get('values')) for f in graph.get('metrics')]),
                    'user_id': user_id,
                    'seconds_since_pedaling_start': graph.get('seconds_since_pedaling_start')
                }

                table = boto3.resource('dynamodb').Table('peloton_graph_data')
                # The info comes in as a float and Dynamo gets mad so just parse it out and make it a json obj
                ddb_data = json.loads(json.dumps(graph_data), parse_float=Decimal)
                # Toss the json into Dynamo

                if save is True:
                    table.put_item(Item=ddb_data)


            if rides is not None:
                try:
                    if workout_hash in rides:
                        continue
                except Exception as e:
                    print(e)

            achievements_url = f"https://api.onepeloton.com/api/user/{user_id}/achievements"
            achievements = self.get(achievements_url, cookies)
            achievements = [f for f in [a.get("achievements") for a in achievements.get("categories")]]
            total_achievements = sum([val.get("count") for sublist in achievements for val in sublist])

            # Performance Graph For that workout/ride
            performance_url = f"https://api.onepeloton.com/api/workout/{workout_id}/performance_graph?every_n=5"
            performance_res = self.get(performance_url, cookies)

            results = {}

            # Each of the averages (Cadence, Speed, Distance, Etc) are in the different summaries
            # So just loop over and grab out the data
            # There are some dupes like heart_rate/achievements_etc but wasn't sure where to put it
            averages = performance_res.get("average_summaries")
            heart_rate = {}
            for average in averages:
                try:
                    heart_rate = [f for f in performance_res.get("metrics")
                                  if f.get("display_name") == "Heart Rate"] or None
                    result = {
                        'name': average.get('display_name', None),
                        'unit': average.get('display_unit', None),
                        'value': average.get('value', None),
                        'distance': [f for f in performance_res.get("summaries")
                                     if f.get("display_name", []) == 'Distance'][0].get("value", None),
                        'heart_rate': heart_rate[0].get("average_value") if heart_rate is not None else None,
                        'total_achievements': total_achievements,
                        'miles_ridden':
                            [f for f in performance_res.get("summaries") if f.get("display_name") == "Distance"][
                                0].get("value", None),
                        'user_id': user_id
                    }
                    results[average.get('display_name')] = result
                except Exception as e:
                    print(e)

            # At some point it would behove me to purge the dynamo db and move the dupes out of results
            # But for now, we will leave it.  Also, account for no heart rate monitor

            miles_ridden = None
            # Need to move some of these around to better error handle the json
            try:
                miles_ridden = [f for f in performance_res.get("summaries")
                                if f.get("display_name") == "Distance"][0].get("value")
            except IndexError:
                miles_ridden = None

            heart_rate = None
            try:
                heart_rate = heart_rate[0].get("average_value", 0)
            except Exception:
                heart_rate = None

            if heart_rate is None:
                try:
                    heart_rate = [f.get('average_value') for f in graph.get('metrics')
                                  if f.get('display_name') == 'Heart Rate']
                except Exception:
                    heart_rate = 0


            my_json_record = {
                "Avg Cadence": results.get("Avg Cadence"),
                "Avg Output": results.get("Avg Output"),
                "Avg Resistance": results.get("Avg Resistance"),
                "Avg Speed": results.get("Avg Speed"),
                'heart_rate': heart_rate,
                'total_achievements': total_achievements,
                'miles_ridden': miles_ridden,
                "created_at": str(created_at),
                "ride_Id": str(created_at),
                'workout_hash': str(workout_hash),
                'user_id': user_id,
                'peloton_id': workout.get('ride').get('live_stream_id')
            }

            table = boto3.resource('dynamodb').Table('peloton_ride_data')
            # The info comes in as a float and Dynamo gets mad so just parse it out and make it a json obj
            ddb_data = json.loads(json.dumps(my_json_record), parse_float=Decimal)
            # Toss the json into Dynamo

            try:
                if save is True:
                    table.put_item(Item=ddb_data)
            except Exception as e:
                print(e)

        # This is just a sanity check coming back from Dynamo

    def get_ride_history(self, user_id=None, ride_id=None):
        """
        So what we're going to do here is pull out all the rides a user took
        Then get a set (unique rides) and then see if a ride they've passed in
        was taken another time

        For instance ride_id [1605579426] is exact same ride as [1603761858]
        And we can see that all of our metrics improved from ride one to ride two
        So we want to pull this out so the user can see it

        What we'll have to do is return this info and then generate the chart(s) from this information.
        I guess what we'll do for now is just show the last two times we took it.  But we'll deal with that problem
        on the front-end.  The only job right here is to just do the data dump
        :param user_id:
        :param ride_id:
        :return:
        """

        table = dynamodb.Table('peloton_ride_data')
        response = table.query(
            IndexName="user_id-index",
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        ride_data = response['Items']
        ride_history_dict = {}
        try:
            ride_ids = [r.get('peloton_id') for r in ride_data]
            user_rides = [r for r in ride_data]
            for ride in ride_ids:
                workout_hash = [u.get('workout_hash') for u in user_rides if u.get('peloton_id') == ride]
                __ride__ = [u.get('ride_Id') for u in user_rides if u.get('peloton_id') == ride]
                ride_history_dict[ride] = (workout_hash, __ride__)
        except Exception as e:
            print(e)

        ride_id_to_workout_hash = [f[1][0] for f in ride_history_dict.items() if ride_id in f[1][1]]
        flat_list = [item for sublist in ride_id_to_workout_hash for item in sublist]

        return flat_list

    def get_achievements(self, user_id=None, cookies=None):
        return PelotonConnection.__get_achievements__(self,user_id=user_id, cookies=cookies)

    '''
    Similar to the get_most_recent_ride this will go and grab the most recent record
    Flip it out to a loop if you want to grab it all
    '''

    def get_most_recent_ride_info(self, user_id=None, cookies=None, save=False):
        workout_ids = PelotonConnection.__get_workouts__(self, user_id, cookies)
        workout_hash_list = []

        """
        TODO: To clear some tech debt start adding the ride_id to the previous entries or find a way to 
        better do this.  I can't be itterating hundreds of rides
        """
        averages = self.__get_user_workouts_by_key__(user_id)
        rides = None
        try:
             rides = [f for f in averages]
             rides = [r for r in rides[0].get('ride_list')]
        except Exception:
             rides = None

        for workout_id in workout_ids:
            workout_url = f"https://api.onepeloton.com/api/workout/{workout_id}"
            workout = self.get(workout_url, cookies)
            created_at = workout.get("created_at")

            d = {
                'created_at': created_at,
                'workout_id': workout.get('id'),
                'bike_id': workout.get('peloton_id')
            }

            dhash = hashlib.md5()
            encoded = json.dumps(d, sort_keys=True).encode()
            dhash.update(encoded)
            workout_hash = dhash.hexdigest()

            try:
                if workout_hash in rides:
                     continue
            except Exception:
                print('')

            # Then get the ride_id for that workout
            ride_id = workout.get("ride").get("id")
            ride_id_details_url = f"https://api.onepeloton.com/api/ride/{ride_id}/details"
            ride_id_details = self.get(ride_id_details_url, cookies)

            # In the event you did one of those non-workout rides
            try:
                instructor = ride_id_details.get('ride').get('instructor').get('name')
            except Exception:
                instructor = None

            if instructor is None:
                instructor = "Free Ride"

            if instructor is not None:
                table = boto3.resource('dynamodb').Table('peloton_course_data')
                if save is True:
                    workout_hash_list.append(workout_hash)
                    table.put_item(
                        Item={
                            "created_at": str(created_at),
                            "difficulty": str(ride_id_details.get('ride', {}).get('difficulty_rating_avg', "N/A")),
                            "instructor": instructor,
                            "length": str(time.strftime("%H:%M:%S", time.gmtime(
                                ride_id_details.get('ride', {}).get('duration', workout.get('end_time') - workout.get(
                                    'created_at'))))),
                            "name": ride_id_details.get('ride', {}).get('title', workout.get('title')),
                            "workout_hash": str(workout_hash),
                            'user_id': user_id
                        }
                    )

            # Also people wanted the music
            if instructor is not None:
                song_list = [song for song in ride_id_details.get("playlist", {}).get("songs", {})]
                set_list = [f"{f.get('title')} by {f.get('artists', None)[0].get('artist_name', None)}" for f in
                            song_list]

                table = boto3.resource('dynamodb').Table('peloton_music_sets')
                if save is True:
                    table.put_item(
                        Item={
                            "created_at": str(created_at),
                            "set_list": set_list,
                            'user_id': user_id,
                            "workout_hash": str(workout_hash),
                        }
                    )

        ride_item = {
            'user_id': user_id,
            'ride_list': workout_hash_list
        }
        ddb_data = json.loads(json.dumps(ride_item))
        table = boto3.resource('dynamodb').Table('peloton_user')
        table.put_item(Item=ddb_data)

    def dump_table(self, table_name):
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

    @staticmethod
    def __get_user_workouts__(user_id=None):
        response = client.get_item(
            TableName="peloton_user",
            Key={
                'user_id': {'S': user_id}
            }
        )
        return response

    @staticmethod
    def __get_user_workouts_by_key__(user_id=None):
        table = dynamodb.Table('peloton_user')
        response = table.query(
            IndexName="user_id-index",
            KeyConditionExpression=Key('user_id').eq(user_id)
        )

        return response['Items']



