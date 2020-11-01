from connection.peloton_connection import PelotonConnection

data = '{"username_or_email":"NONE","password":"NONE"}'

# Create the Peloton Connection
conn = PelotonConnection()

# Connect and return the info to create the cookie
auth_response = conn.post("https://api.onepeloton.com/auth/login", data)
session_id = auth_response.get("session_id")
user_id = auth_response.get("user_id")

# Create the cookie
cookies = dict(peloton_session_id=session_id)

# Run this daily or set-up a cron to do it for you
conn.get_most_recent_ride_details(user_id, cookies, False)
conn.get_most_recent_ride_info(user_id, cookies, False)
