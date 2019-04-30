import pymongo
import config
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import re

LOCAL_CLIENT = pymongo.MongoClient()
db = LOCAL_CLIENT.tweets

LOCAL_DB = config.LOCAL_CLIENT
ATLAS_DB = config.ATLAS_CLIENT.tweets
NUM_DOCS = ATLAS_DB.tweet_dicts.count_documents({"id":{"$exists":True}})

GEOSERVICE = Nominatim(user_agent="geocoder")
GEOCODER = RateLimiter(GEOSERVICE.geocode, min_delay_seconds=2, max_retries=3)

# test_locations = ['washington dc',
#                   'london england',
#                   'london uk',
#                   'london',
#                   'uk',
#                   'germany berlin',
#                   'armenia',
#                   'us',
#                   'usa',
#                   'w1a 4ww',
#                   'Milano, Siracusa, Paris, Warszawa, Firenze, Coventry...',
#                   '....further north',
#                   'hyères, france',
#                   'portugal - ahh...',
#                   'london/ naples',
#                   'tx',
#                   'united states',
#                   'جمهوری اسلامی ایران',
#                   'otranto ,italy',
#                   'world wide web']
#
# test_doc = {}

def get_df():

    ###ADD SOME SORT OF LOGIC THAT PREVENTS PULLING THE DF IF THE # of DOCS IS NOT GREATER THAN 1


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

# def nominatim_fetcher(loc, counter=3):
#     while counter > 0:
#         try:
#             return Nominatim(user_agent="geocoder").geocode(loc)
#         except GeocoderTimedOut:
#             time.sleep(2)
#             return nominatim_fetcher(loc, counter-1)

def location_importance(loc_obj):
    try:
        return loc_obj.raw['importance']

    except (AttributeError, KeyError) as e:
        return 0.0

def get_coords(loc_obj):
    try:
        return [float(loc_obj.raw['lat']), float(loc_obj.raw['lon'])]

    except (AttributeError, KeyError) as e:
        return [0.0, 0.0]

def get_bounding_box(loc_obj):
    try:
        return loc_obj.raw['boundingbox']

    except (AttributeError, KeyError) as e:
        return None

def add_loc_data():

    df = clean_data()

    df['geocode'] = df['clean_location'].apply(GEOCODER)
    # df['coordinates'] = df['geocode'].apply(lambda loc_obj: tuple(loc_obj.point) if loc_obj else None)
    df['coordinates'] = df['geocode'].apply(get_coords)
    df['bounding_box'] = df['geocode'].apply(get_bounding_box)
    df['importance_score'] = df['geocode'].apply(location_importance)
    # return df
    df.to_csv('sandbox/test_output.csv', mode='w')
    print('printed to csv')

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
