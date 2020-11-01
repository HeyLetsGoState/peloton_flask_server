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
        my_workouts_url = f"https://api.onepeloton.com/api/user/{user_id}/workouts"
        my_workouts = self.get(my_workouts_url, cookies)

        # Get my workout ids ONLY for the bike
        my_workouts = [w for w in my_workouts.get("data") if w.get("fitness_discipline") == "cycling"]
        workout_ids = [workout_id.get("id") for workout_id in my_workouts]
        return workout_ids

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
            for average in averages:
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
                        0].get("value")
                }
                results[average.get('display_name')] = result

            # At some point it would behove me to purge the dynamo db and move the dupes out of results
            # But for now, we will leave it.  Also, account for no heart rate monitor
            my_json_record = {
                "Avg Cadence": results.get("Avg Cadence"),
                "Avg Output": results.get("Avg Output"),
                "Avg Resistance": results.get("Avg Resistance"),
                "Avg Speed": results.get("Avg Speed"),
                'heart_rate': heart_rate[0].get("average_value") if heart_rate is not None else None,
                'total_achievements': total_achievements,
                'miles_ridden': [f for f in performance_res.get("summaries") if f.get("display_name") == "Distance"][
                    0].get("value"),
                "ride_Id": str(created_at)
             }

            table = boto3.resource('dynamodb').Table('peloton_ride_data')
            # The info comes in as a float and Dynamo gets mad so just parse it out and make it a json obj
            ddb_data = json.loads(json.dumps(my_json_record), parse_float=Decimal)
            # Toss the json into Dynamo

            if save is True:
                table.put_item(Item=ddb_data)

        # This is just a sanity check coming back from Dynamo


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

            if instructor is not None:
                table = boto3.resource('dynamodb').Table('peloton_course_data')
                if save is True:
                    table.put_item(
                        Item={
                            "created_at": str(created_at),
                            "difficulty": str(ride_id_details.get('ride').get('difficulty_rating_avg')),
                            "instructor": instructor,
                            "length": str(time.strftime("%H:%M:%S", time.gmtime(ride_id_details.get('ride').get('duration')))),
                            "name": ride_id_details.get('ride').get('title')
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
                            "set_list": set_list
                        }
                    )

