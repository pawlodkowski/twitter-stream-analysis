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

    # cursor = ATLAS_DB.tweet_dicts.find({"id":{"$exists":True}})
    # cursor = ATLAS_DB.tweet_dicts.find_one()
    # cursor = ATLAS_DB.tweet_dicts.find({"num_followers":{"$gt":25000000}})
    cursor = ATLAS_DB.tweet_dicts.aggregate([{ "$sample": {"size": 20}}])
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
    # while counter > 0:
    try:
        return GEOSERVICE.geocode(loc,
                                exactly_one=True,
                                timeout=2,
                                language='en',
                                addressdetails=False)

    except GeocoderTimedOut:
        # time.sleep(2)
        # return geocode_fetcher(loc, counter-1)
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


class CustomEncoder():
    def __init__(self, x):
        self.__x = x

    def x(self):
        return self.__x

def encode_custom(custom):
    return {"_type": "custom", "x": custom.x()}

def get_geocoded_object(clean_location):

    if LOCAL_DB.geocodes.count_documents({"clean_location": clean_location}) >=1:
        geocoded_object = LOCAL_DB.geocodes.find_one({"clean_location": clean_location})["geocoded_object"]
        # return geocoded_object['loc_obj']
        return geocoded_object

    else:
        # geocoded_object = {'loc_obj': geocode_fetcher(clean_location)}
        geocoded_object = geocode_fetcher(clean_location)

        print(f"\n\nDATATYPE: {type(geocoded_object)}\n\n")

        LOCAL_DB.geocodes.insert_one({
                                      "clean_location" : clean_location,
                                      "geocoded_object" : encode_custom(CustomEncoder(geocoded_object)),
                                      # "geocoded_name" : get_geocoded_name(geocoded_object['loc_obj']),
                                      # "coordinates" : get_coords(geocoded_object['loc_obj']),
                                      # "bounding_box" : get_bounding_box(geocoded_object['loc_obj']),
                                      # "importance" : get_importance_score(geocoded_object['loc_obj'])
                                      })

        # return geocoded_object['loc_obj']
        return geocoded_object


def add_loc_data():

    print("building dataframe and cleaning data...\n\n")
    df = clean_data()

    print("getting geocodes from Nominatum / local database...\n\n")
    df['geocoded_object'] = df['clean_location'].apply(get_geocoded_object)
    df['geocoded_name'] = df['geocoded_object'].apply(lambda g: g[0] if g else None)

    print("getting coordinates...\n\n")
    df['coordinates'] = df['geocoded_object'].apply(get_coords)

    print("getting bounding box...\n\n")
    df['bounding_box'] = df['geocoded_object'].apply(get_bounding_box)

    print("getting importance score...\n\n")
    df['importance_score'] = df['geocoded_object'].apply(get_location_importance)

    # return df
    with open('sandbox/test_output.csv', 'a') as f:
        df.to_csv(f, header=False)
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

# def get_geocode(address):
#        address = address.lower() #+ furter preprocessing?!
#        if db.geocodes.count_documents({"raw": address}) >= 1:
#            geocode = db.geocodes.find_one({"raw": address})["geocode"]
#            return geocode
#        else:
#            try:
#                geocode = list(Nominatim(user_agent = "categorizer").geocode(address))
#            except GeocoderTimedOut:
#                time.sleep(5)
#                geocode = list(Nominatim(user_agent = "categorizer").geocode(address))
#            except TypeError:
#                return f"{address} - NO GEOCODE OBTAINED"
#            db.geocodes.insert_one({"raw" : address,
#                                    "geocode" : geocode[0],
#                                    "interpretation" : geocode[0].split(",")[0],
#                                    "coordinates" :geocode[1]})
#            return geocode[0] #--> 0 being the actual geocode, whereas 1 are coordinates.
