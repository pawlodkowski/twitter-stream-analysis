import json

import numpy as np
import pandas as pd
from tweepy import API, OAuthHandler, Stream
from tweepy.streaming import StreamListener
from datetime import datetime #for testing time of script execution

import config
# from search_terms import KEYWORDS_LIST
### ^ No longer importing this from separate file,
#but rather as a direct parameter in TwitterStreamer

STARTTIME = datetime.now()
OUTPUT_FILENAME = 'sandbox/sample_dict_output.json' #used for testing
LANGUAGES = ['en']

class TwitterAuthenticator():
    def authenticate(self):
        auth = OAuthHandler(config.CONSUMER_API_KEY, config.CONSUMER_API_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
        return auth

class TwitterListener(StreamListener):

    """Required Class that inherits from tweepy.StreamListener.
       on_data() method dictates what should be done with tweets
       as soon as they come in contact with the Listener / program."""

    def __init__(self, limit, callback):
        super().__init__()
        self.limit = limit
        self.counter = 0
        self.callback = callback
        # self.output_filename = output_filename

    def on_error(self, status):

        """kill the connection if rate-limiting occurs.
        see: https://developer.twitter.com/en/docs/basics/response-codes"""

        if status == 420:
            return 420
        print(status)

    def get_geo_data(self, t):

        geo_data = []
        if t['place'] != None:
            try:
                geo_data.append(t['place']['full_name'])
                geo_data.append(t['place']['bounding_box']['coordinates'])
            except KeyError:
                geo_data.append(['KeyError'])

        return geo_data

    def get_user_location(self, t):

        user_location = []
        if t['user']['location'] != None:
            try:
                user_location.append(t['user']['location'])
            except KeyError:
                user_location.append(['KeyError'])

        return user_location

    def get_hashtags(self, t):
        hashtags = []
        if 'extended_tweet' in t:
            for hashtag in t['extended_tweet']['entities']['hashtags']:
                hashtags.append(hashtag['text'])
        elif 'hashtags' in t['entities'] and len(t['entities']['hashtags']) > 0:
            hashtags = [item['text'] for item in t['entities']['hashtags']]

        return hashtags

    def get_tweet_dict(self, t):

        '''extract relevant information from the tweet and structure into a dictionary'''

        if 'extended_tweet' in t:
            text = t['extended_tweet']['full_text']
        else:
            text = t['text']

        geo_data = self.get_geo_data(t)
        user_location = self.get_user_location(t)
        hashtags = self.get_hashtags(t)

        tweet = {'id': t['id_str'],
                 'tweet_created_at': t['created_at'],
                 'text': text,
                 'user': t['user']['screen_name'],
                 'source': t['source'],
                 'language': t['lang'],
                 'user_description': t['user']['description'],
                 'num_followers':t['user']['followers_count'],
                 'user_statuses': t['user']['statuses_count'],
                 'user_created_at': t['user']['created_at'],
                 'hashtags': hashtags,
                 'tweet_location': geo_data,
                 'user_location': user_location,
                 }
        return tweet

    def on_data(self, data):

        '''
           Define what should be done with each incoming streamed tweet as it
           is intercepted by the StreamListener:
           - convert each json-like string from twitter into a workable JSON object;
           - ignore retweets, replies, and quoted tweets;
           - apply the get_tweet_dict function to each object;
           - apply a callback function to the resulting dictionary;
           - shut off StreamListener as soon as it reaches a pre-defined limit.
        '''

        t = json.loads(data)

        if 'RT' not in t['text'] and t['in_reply_to_status_id'] == None and t['is_quote_status'] == False:

            tweet = self.get_tweet_dict(t)
            self.callback(tweet)

            #commented out, since output_filename was just used for testing.
            # with open(self.output_filename, 'a') as file:
            #     file.write(str(tweet))

            self.counter += 1

            if self.counter == self.limit:
                return False

class TwitterStreamer():
    '''
       Class containing the primary method / functionality of the script.
    '''

    def __init__(self, keywords):
        self.twitter_authenticator = TwitterAuthenticator()
        self.keywords = keywords

    def stream_tweets(self, limit, callback):
        listener = TwitterListener(limit, callback)
        auth = self.twitter_authenticator.authenticate()
        stream = Stream(auth, listener)
        stream.filter(track=self.keywords, languages=LANGUAGES)


if __name__ == "__main__":

    twitter_streamer = TwitterStreamer(['kanyewest'])
    twitter_streamer.stream_tweets(10, print)
