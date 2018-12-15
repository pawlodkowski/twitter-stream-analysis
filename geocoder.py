import pymongo
import config
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut
import re

LOCAL_DB = config.LOCAL_CLIENT.tweets
ATLAS_DB = config.ATLAS_CLIENT.tweets
NUM_DOCS = ATLAS_DB.tweet_dicts.count_documents({"id":{"$exists":True}})

GEOSERVICE = Nominatim(user_agent="geocoder")
GEOCODER = RateLimiter(GEOSERVICE.geocode, min_delay_seconds=2, max_retries=3)
#for applying to dataframe

def get_df():

    """Still a work in progress: toggle the different queries to test different
       types of queries. Ultimately, I will need to run .find({"id":{"$exists":True}})
       in order to get geocoded data for ALL tweets in the database."""

    # cursor = ATLAS_DB.tweet_dicts.find({"id":{"$exists":True}})
    # cursor = ATLAS_DB.tweet_dicts.find_one()
    # cursor = ATLAS_DB.tweet_dicts.find({"num_followers":{"$gt":25000000}})
    cursor = ATLAS_DB.tweet_dicts.aggregate([{ "$sample": {"size": 50}}]) #random sample of 20 tweets
    # cursor = ATLAS_DB.tweet_dicts.find({"num_followers": {"$elemMatch": {"$gte": 400, "$lt": 600}}})

    df = pd.DataFrame(list(cursor))
    df.set_index('id', inplace=True)
    df.drop('_id', axis=1, inplace=True)

    extracted_locations = []
    for u in df['user_location']:
        if u:
            extracted_locations.append(u[0])
        else:
            extracted_locations.append("")

    df['user_location'] = extracted_locations

    return df

def clean_data():

    clean = []
    df = get_df()

    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

    emoji_pattern2 = re.compile(u'('
        u'\ud83c[\udf00-\udfff]|'
        u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
        u'[\u2600-\u26FF\u2700-\u27BF])+',
        re.UNICODE)

    re_urls = 'http[s]?:\/\/(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+'
    re_at_mentions = '(?:@[\w_]+)'
    re_emails ='\S*@\S*\s?'
    re_other_special_chars = '\\|\r|\n|\s|\"|\[|\]|\{|\}|\;|\:|\?|\!|\.|\°|\-|\/|\&|\(|\)|\||\*'
    re_inside_parenthesis = '\([^)]*\)'
    re_full_hashtags = '#(\w+)'
    re_extra_white_space = '\s+'

    for text in df['user_location']:
        text = re.sub(emoji_pattern, ' ', text)
        text = re.sub(emoji_pattern2, ' ', text)
        text = re.sub(re_urls, ' ', text)
        text = re.sub(re_emails, ' ', text)
        text = re.sub(re_at_mentions, ' ', text)
        text = re.sub(re_inside_parenthesis, ' ', text)
        text = re.sub(re_other_special_chars, ' ', text)
        text = re.sub(re_full_hashtags, ' ', text)
        text = text.lower()
        text = text.strip()
        text = re.sub(re_extra_white_space, ' ', text)

        clean.append(text)

    df['clean_location'] = clean

    return df

def geocode_fetcher(loc, counter=3):

    try:
        return GEOSERVICE.geocode(loc,
                                exactly_one=True,
                                timeout=2,
                                language='en',
                                addressdetails=False)

    except GeocoderTimedOut:

        print(f"GEOCODER TIMED OUT FOR: {loc}")
        return None

    except (AttributeError, KeyError, TypeError) as e:

        print(f"ATTRIBUTE / KEY / TYPE ERROR FOR: {loc}")
        return None

def get_importance_score(loc_obj):
    try:
        return loc_obj.raw['importance']

    except (AttributeError, KeyError) as e:
        return 0.0

def get_coords(loc_obj):
    try:
        return [loc_obj.latitude, loc_obj.longitude]

    except (AttributeError, KeyError) as e:
        return [0.0, 0.0]

def get_bounding_box(loc_obj):
    try:
        return loc_obj.raw['boundingbox']

    except (AttributeError, KeyError) as e:
        return None

def get_geocoded_name(loc_obj):
    try:
        return loc_obj.address

    except (AttributeError, KeyError) as e:
        return None

"""custom encoder (commented out below) was an attempt to "mask" a geopy.location.Location data type
   into a  another data structure, so it could be inserted into MongoDB -- but this still did not work.
   MongoDB's BSON decoder absolutely refuses to accept that data type."""

# class CustomEncoder():
#     def __init__(self, x):
#         self.__x = x
#
#     def x(self):
#         return self.__x
#
# def encode_custom(custom):
#     return {"_type": "custom", "x": custom.x()}

def get_geocoded_object(clean_location):

    """
        If the entry already exists in the local database, then
        simply return the geo object from the local "cache."
        Otherwise, fetch the data from the Nominatim API, and store it
        in the local cache afterwards.
    """

    if LOCAL_DB.geocodes.count_documents({"geocoded_object.clean_location": clean_location}) >=1:

        geocoded_object = LOCAL_DB.geocodes.find_one({'geocoded_object.clean_location': clean_location})
        geocoded_object = geocoded_object['geocoded_object'] #strip off the ObjectId field from MongoDB

        return geocoded_object

    else:

        geopyLocation = geocode_fetcher(clean_location)

        # if geopyLocation: #if geocode_fetcher did not return a None type or an empty object.

        geocoded_object = {
                          "clean_location" : clean_location,
                          "geocoded_name" : get_geocoded_name(geopyLocation),
                          "coordinates" : get_coords(geopyLocation),
                          "bounding_box" : get_bounding_box(geopyLocation),
                          "importance" : get_importance_score(geopyLocation)
                          }
        # else:
        #     geocoded_object = {
        #                       "clean_location" : clean_location,
        #                       "geocoded_name" : None,
        #                       "coordinates" : None,
        #                       "bounding_box" : None,
        #                       "importance" : None
        #                       }

        # geocoded_object_encoded = {'loc_obj': geocode_fetcher(clean_location)}
        # geocoded_object_encoded = encode_custom(CustomEncoder(geocoded_object))

        LOCAL_DB.geocodes.insert_one({"geocoded_object" : geocoded_object})

        """ERROR -- Pymongo / BSON refuses to accept the geocoded object in its entirety,
        (data type: geopy.location.Location), no matter how much I try to "encode" it into other data structures, such as within a dictionary or embedded within a class Method.

        That is why I am extracting everything I need from the geopy object first,
        and storing its elements into a Python dictionary, which the DB accepts as a valid structure."""

        return geocoded_object


def add_loc_data():

    print("building dataframe and cleaning data...\n\n")
    df = clean_data()

    print("getting geocoded objects from Nominatim / local database...\n\n")
    df['geocoded_object'] = df['clean_location'].apply(get_geocoded_object)

    print("getting geocoded names...\n\n")
    df['geocoded_name'] = df['geocoded_object'].map(lambda x: x.get('geocoded_name') if x else None)

    print("getting coordinates...\n\n")
    df['coordinates'] = df['geocoded_object'].map(lambda x: x.get('coordinates') if x else None)

    print("getting bounding box...\n\n")
    df['bounding_box'] = df['geocoded_object'].map(lambda x: x.get('bounding_box') if x else None)

    print("getting importance score...\n\n")
    df['importance_score'] = df['geocoded_object'].map(lambda x: x.get('importance') if x else None)

    # # return df

    with open('sandbox/test_output.csv', 'a') as f:
        df.to_csv(f, header=True)
    print("Successfully printed to csv!\n\n")

def test_geo_cleaning():

    df = get_df()
    dirty = df['user_location']
    clean = clean_data()

    for d, c in zip(dirty, clean):
        print(f"{d:<40}, {c:<40}")

print(f"doc counts: {NUM_DOCS}\n")
# test_geo_cleaning()
add_loc_data()
