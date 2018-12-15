import pymongo

"""
   Variables that contain user credentials for Twitter API and Mongo Atlas DB.

    Intructions:

    - paste your own Twitter credentials and Mongo Atlas Cloud DB
     username / password into the empty strings.

    - you can find the SHORT_SRV_SUFFIX by going into your Mongo Atlas DB cluster
     and clicking 'Connect' > 'Connect your application' and copying the part
     of the 'Short SRV connection string' after the '@' symbol.
     (e.g. 'myproject-rakqi.mongodb.net/test?retryWrites=true')

    - the ATLAS_CLIENT is used for storing twitter data in the remote database,
    and the LOCAL_CLIENT is used for storing a local version of a "lookup" table
    of location names that are queried and geo-coded from the Nominatum API tool
    (https://wiki.openstreetmap.org/wiki/Nominatim). This variable is used in the geocoder.py file.

        - The purpose of this strategy is to keep a local "cache" of geo-coded
        locations so that this program limits the amount of times it needs to
        ping the Nominatim API through the geopy library (i.e. whenever the program
        is in the process of geo-coding the user location field of each collected tweet,
        it will first check if the user location is in the local database;
        if it's not, only then will it send a request to the API.)
"""


### TWITTER ###

CONSUMER_API_KEY = ""
CONSUMER_API_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

### MONGO ATLAS CLOUD DB ###

DB_USERNAME = ""
DB_PASSWORD = ""
SHORT_SRV_SUFFIX = ""

ATLAS_CLIENT = pymongo.MongoClient(f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@{SHORT_SRV_SUFFIX}")
LOCAL_CLIENT = pymongo.MongoClient()
