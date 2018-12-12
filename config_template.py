import pymongo

"""
   Variables that contain user credentials for Twitter API and Mongo Atlas DB.

   Intructions:

   - paste your own Twitter credentials and Mongo Atlas Cloud DB
     username / password into the empty strings.

   - you can find the short_srv_suffix by going into your Mongo Atlas DB cluster
     and clicking 'Connect' > 'Connect your application' and copying the part
     of the 'Short SRV connection string' after the '@' symbol.
     (e.g. 'myproject-rakqi.mongodb.net/test?retryWrites=true')
"""


### TWITTER ###

CONSUMER_API_KEY = ""
CONSUMER_API_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

### MONGO ATLAS CLOUD DB ###

db_username = ""
db_password = ""
short_srv_suffix = ""

client = pymongo.MongoClient(f"mongodb+srv://{db_username}:{db_password}@{short_srv_suffix}")
