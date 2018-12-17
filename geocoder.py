import pymongo
import config
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut
import re
from datetime import datetime #for testing time of script execution
import os

STARTTIME = datetime.now()

LOCAL_DB = config.LOCAL_CLIENT.tweets
ATLAS_DB = config.ATLAS_CLIENT.tweets
NUM_DOCS = ATLAS_DB.tweet_dicts.count_documents({"id":{"$exists":True}})

GEOSERVICE = Nominatim(user_agent="geocoder")
GEOCODER = RateLimiter(GEOSERVICE.geocode, min_delay_seconds=2, max_retries=3)
#for applying to dataframe

class PrepareData():

    def get_df(self):

        cursor = ATLAS_DB.tweet_dicts.aggregate([{ "$sample": {"size": 1000}}]) #random sample 1000 tweets

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

    def clean_data(self):

        clean = []

        df = self.get_df()

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

        geocoded_object = {
                          "clean_location" : clean_location,
                          "geocoded_name" : get_geocoded_name(geopyLocation),
                          "coordinates" : get_coords(geopyLocation),
                          "bounding_box" : get_bounding_box(geopyLocation),
                          "importance" : get_importance_score(geopyLocation)
                          }

        LOCAL_DB.geocodes.insert_one({"geocoded_object" : geocoded_object})

        return geocoded_object


def main():

    print("building dataframe and cleaning data...\n\n")
    df = PrepareData().clean_data()

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

    """Will eventually return the resulting df, but for now printing it to csv for testing"""

    test_filename = 'sandbox/test_output.csv'
    if os.path.exists(test_filename):
        print('\n\nalready exists; removing it first')
        os.remove(test_filename)

    with open(test_filename, 'w') as f:
        df.to_csv(f, header=True)
    print("Successfully printed to csv!\n\n")


print(f"\n\nNumber of documents currently in DB: {NUM_DOCS}\n")

main()

print(f"Time to completion: {datetime.now() - STARTTIME}")
