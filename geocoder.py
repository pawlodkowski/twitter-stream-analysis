import pymongo
import config
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut
import re
from datetime import datetime #for testing time of script execution
import os
import time
import pickle
from shapely.geometry import Polygon, MultiPolygon, Point

STARTTIME = datetime.now()

LOCAL_DB = config.LOCAL_CLIENT.tweets
ATLAS_DB = config.ATLAS_CLIENT.tweets
# NUM_DOCS = ATLAS_DB.tweet_dicts.count_documents({"id":{"$exists":True}}) #OLD SETUP FOR AWS SERVER
NUM_DOCS = LOCAL_DB.tweet_dicts.count_documents({"id":{"$exists":True}})

GEOSERVICE = Nominatim(user_agent="personal-project")
GEOSERVICE_RL = RateLimiter(GEOSERVICE.geocode, min_delay_seconds=5, max_retries=3)
#for applying to dataframe

INPUT_FILE = 'data/merged_polygon_dict'
#for importing polygon dictionary created in 'generate_polygon_dict.py'

class PrepareData():

    def get_df(self):

        # cursor = ATLAS_DB.tweet_dicts.aggregate([{ "$sample": {"size": 50}}]) #OLD SETUP FOR AWS SERVER
        # cursor = ATLAS_DB.tweet_dicts.find({"id":{"$exists":True}}) #ALL OF THEM
        cursor = LOCAL_DB.tweet_dicts.find({"id":{"$exists":True}})

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

        clean_user_location = []
        clean_source = []
        hashtag_strings = []

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
        re_parse_source = "\>(.+)\<" #to remove the html divs from ['source']

        for text in df['user_location']:
            text = re.sub(emoji_pattern, ' ', str(text))
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

            clean_user_location.append(text)

        for text in df['source']:
            text = re.findall(re_parse_source, text)[0] #because it returns a list
            text = text.strip()

            clean_source.append(text)

        for hashtag_list in df['hashtags']:
            if hashtag_list: #if not an empty list
                new_string = ' '.join(hashtag_list)
            else:
                new_string = None
            hashtag_strings.append(new_string)


        df['clean_location'] = clean_user_location
        df['source'] = clean_source
        df['hashtags'] = hashtag_strings

        return df

def geocode_fetcher(loc):

    try:

        print(f'pinging Nominatim for: {loc}\nWaiting 1 second before next request...\n\n')
        time.sleep(1) #add second of delay between each request to avoid Rate Limiting

        return GEOSERVICE.geocode(loc,
                                exactly_one=True,
                                timeout=2,
                                language='en',
                                addressdetails=False)


    except GeocoderTimedOut:

        print(f"\n\n\n\n----------------GEOCODER TIMED OUT FOR: {loc}----------------\n\n\n\n")
        return None

    except (AttributeError, KeyError, TypeError) as e:

        print(f"\n\n\n\n----------------ATTRIBUTE / KEY / TYPE ERROR FOR: {loc}----------------\n\n\n\n")
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

    print("\n\ngetting geocoded names...\n\n")
    df['geocoded_name'] = df['geocoded_object'].map(lambda x: x.get('geocoded_name') if x else None)

    print("getting coordinates...\n\n")
    df['coordinates'] = df['geocoded_object'].map(lambda x: x.get('coordinates') if x else None)
    df['latitude'] = df['coordinates'].map(lambda x: x[0] if x else None)
    df['longitude'] = df['coordinates'].map(lambda x: x[1] if x else None)

    print("getting bounding box...\n\n")
    df['bounding_box'] = df['geocoded_object'].map(lambda x: x.get('bounding_box') if x else None)

    print("getting importance scores...\n\n")
    df['importance_score'] = df['geocoded_object'].map(lambda x: x.get('importance') if x else None)

    ### ADDITIONS FOR SHAPELY WORK ###

    print("trimming out bad geo data...\n\n")
    df = df[df['importance_score'] > 0.6] #trim out all the non-important geo-data

    """Conditional statements for identifying ambiguous locations (which would skew mapping)"""

    COND1 = (df['geocoded_name'] != 'USA')
    COND2 = (df['geocoded_name'] != 'Canada')
    COND3 = (df['geocoded_name'] != 'Europe')
    COND4 = (df['geocoded_name'] != 'North America')
    COND5 = (df['geocoded_name'] != 'United Kingdom')
    COND6 = (df['geocoded_name'] != 'England, United Kingdom')
    COND7 = (df['geocoded_name'] != 'Scotland, United Kingdom')
    COND8 = (df['geocoded_name'] != 'Wales, United Kingdom')
    COND9 = (df['geocoded_name'] != 'Northern Ireland, United Kingdom')

    df = df[COND1 & COND2 & COND3 & COND4 & COND5 & COND6 & COND7 & COND8]

    print("importing polygon dictionary for mapping...\n\n")
    with open(INPUT_FILE, 'rb') as file:
        merged_polygon_dict = pickle.load(file)

    coords = []
    country_matched = []
    country_matched_RP_lat = []
    country_matched_RP_lon = []
    country_area = []

    print("mapping each set of lat / long to its proper GeoJSON feature...\n\n")
    for long, lat in zip(list(df['longitude']), list(df['latitude'])):
        coords.append(Point(long, lat))

    for p in coords:
        default = 'Unknown'

        for name, polygon in merged_polygon_dict.items():
            if polygon.contains(p):

                default = name
                marker_lat = polygon.representative_point().y
                marker_lon = polygon.representative_point().x
                area = polygon.area
                break #as soon as it find a match, break out of the loop.

        country_matched.append(default)
        country_matched_RP_lat.append(marker_lat)
        country_matched_RP_lon.append(marker_lon)
        country_area.append(area)

    df['GeoRegion'] = country_matched
    df['GeoRegion_RP_Lat'] = country_matched_RP_lat
    df['GeoRegion_RP_Lon'] = country_matched_RP_lon
    df['GeoRegion_Area'] = country_area

    cols_to_drop = ['language',
                    # 'user',
                    'user_location',
                    'geocoded_object',
                    'geocoded_name',
                    'coordinates',
                    'bounding_box',
                    'tweet_location'
                    ]

    for col in cols_to_drop:
        df.drop(col, axis=1, inplace=True)


    # """Will eventually return the resulting df, but for now printing it to csv for testing"""
    #
    # test_filename = 'sandbox/live_demo.csv'
    # if os.path.exists(test_filename):
    #     print(f'{test_filename} already exists; removing it first\n')
    #     os.remove(test_filename)
    #
    # with open(test_filename, 'w') as f:
    #     df.to_csv(f, header=True)
    # print("Successfully printed to csv!\n\n")

    return df

if __name__ == '__main__':

    print(f"\n\nNumber of documents currently in DB: {NUM_DOCS}\n")
    main()
    print(f"Time to completion: {datetime.now() - STARTTIME}")
