import hashlib
from hashlib import sha1

import requests
import time
import json
import boto3
from decimal import *


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

    @staticmethod
    def __get_workouts__(self, user_id, cookies):
        # Get my workout information
        page = 0
        my_workouts_url = f"https://api.onepeloton.com/api/user/{user_id}/workouts?page={page}"
        workout_results = []

        my_workouts = self.get(my_workouts_url, cookies)
        workout_results.append(my_workouts)

        if my_workouts.get('show_next') is True:
            page += 1
            my_workouts_url = f"https://api.onepeloton.com/api/user/{user_id}/workouts?page={page}"
            my_workouts = self.get(my_workouts_url, cookies)
            workout_results.append(my_workouts)


        # Get my workout ids ONLY for the bike
        my_workouts = [w for w in workout_results.get("data") if w.get("fitness_discipline") == "cycling"]
        workout_ids = [workout_id.get("id") for workout_id in workout_results]
        return workout_ids

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
        for workout_id in workout_ids:
            workout_url = f"https://api.onepeloton.com/api/workout/{workout_id}"
            # Get the workout info
            workout = self.get(workout_url, cookies)
            created_at = workout.get("created_at")

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
                        'name': average.get('display_name'),
                        'unit': average.get('display_unit'),
                        'value': average.get('value'),
                        'distance': [f for f in performance_res.get("summaries")
                                     if f.get("display_name") == 'Distance'][0].get("value"),
                        'heart_rate': heart_rate[0].get("average_value") if heart_rate is not None else None,
                        'total_achievements': total_achievements,
                        'miles_ridden': [f for f in performance_res.get("summaries") if f.get("display_name") == "Distance"][
                            0].get("value"),
                        'user_id': user_id
                    }
                    results[average.get('display_name')] = result
                except:
                    print("Drop it to the floor")

            """
            Now that more than one user wants to use this thing, we need to make each record super unique
            So we'll take the created at and the workout id and make that the hash.
            We'll combine the time of the ride, the id of the ride and the id of the bike
            """
            d = {
                'created_at': created_at,
                'workout_id': workout.get('id'),
                'bike_id': workout.get('peloton_id')
            }

            dhash = hashlib.md5()
            encoded = json.dumps(d, sort_keys=True).encode()
            dhash.update(encoded)
            workout_hash = dhash.hexdigest()


            # At some point it would behove me to purge the dynamo db and move the dupes out of results
            # But for now, we will leave it.  Also, account for no heart rate monitor

            miles_ridden = None
            # Need to move some of these around to better error handle the json
            try:
                miles_ridden = [f for f in performance_res.get("summaries")
                                if f.get("display_name") == "Distance"][0].get("value")
            except IndexError:
                miles_ridden = None

            my_json_record = {
                "Avg Cadence": results.get("Avg Cadence"),
                "Avg Output": results.get("Avg Output"),
                "Avg Resistance": results.get("Avg Resistance"),
                "Avg Speed": results.get("Avg Speed"),
                'heart_rate': heart_rate[0].get("average_value") if heart_rate is not None else None,
                'total_achievements': total_achievements,
                'miles_ridden': miles_ridden,
                "created_at": str(created_at),
                "ride_Id": str(created_at),
                'workout_hash': str(workout_hash),
                'user_id': user_id
             }

            table = boto3.resource('dynamodb').Table('peloton_ride_data')
            # The info comes in as a float and Dynamo gets mad so just parse it out and make it a json obj
            ddb_data = json.loads(json.dumps(my_json_record), parse_float=Decimal)
            # Toss the json into Dynamo

            if save is True:
                table.put_item(Item=ddb_data)

        # This is just a sanity check coming back from Dynamo

    def get_user_info(self, user_id=None, cookies=None):
        user_info = PelotonConnection.__get_user__(self, user_id, cookies)
        return user_info

    '''
    Similar to the get_most_recent_ride this will go and grab the most recent record
    Flip it out to a loop if you want to grab it all
    '''
    def get_most_recent_ride_info(self, user_id=None, cookies=None, save=False):
        workout_ids = PelotonConnection.__get_workouts__(self, user_id, cookies)
        for workout_id in workout_ids:
            workout_url = f"https://api.onepeloton.com/api/workout/{workout_id}"
            workout = self.get(workout_url, cookies)
            created_at = workout.get("created_at")
            # Then get the ride_id for that workout
            ride_id = workout.get("ride").get("id")
            ride_id_details_url = f"https://api.onepeloton.com/api/ride/{ride_id}/details"
            ride_id_details = self.get(ride_id_details_url, cookies)

            # In the event you did one of those non-workout rides
            try:
                instructor = ride_id_details.get('ride').get('instructor').get('name')
            except Exception:
                instructor = None

            d = {
                'created_at': created_at,
                'workout_id': workout.get('id'),
                'bike_id': workout.get('peloton_id')
            }

            dhash = hashlib.md5()
            encoded = json.dumps(d, sort_keys=True).encode()
            dhash.update(encoded)
            workout_hash = dhash.hexdigest()

            if instructor is not None:
                table = boto3.resource('dynamodb').Table('peloton_course_data')
                if save is True:
                    table.put_item(
                        Item={
                            "created_at": str(created_at),
                            "difficulty": str(ride_id_details.get('ride').get('difficulty_rating_avg')),
                            "instructor": instructor,
                            "length": str(time.strftime("%H:%M:%S", time.gmtime(ride_id_details.get('ride').get('duration')))),
                            "name": ride_id_details.get('ride').get('title'),
                            "workout_hash": str(workout_hash),
                            'user_id': user_id
                        }
                    )

            # Also people wanted the music
            if instructor is not None:
                song_list = [song for song in ride_id_details.get("playlist").get("songs")]
                set_list = [f"{f.get('title')} by {f.get('artists')[0].get('artist_name')}" for f in song_list]

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


