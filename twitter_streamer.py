from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

import config
from search_terms import KEYWORDS_LIST

from tweepy import API

import numpy as np
import pandas as pd

OUTPUT_FILENAME = 'sandbox/sample_dict_output.json'
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

    def __init__(self, limit, callback, output_filename):
        super().__init__()
        self.limit = limit
        self.counter = 0
        self.callback = callback
        self.output_filename = output_filename

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
        else:
            geo_data.append([])

        return geo_data

    def get_user_location(self, t):

        user_location = []
        if t['user']['location'] != None:
            try:
                user_location.append(t['user']['location'])
            except KeyError:
                user_location.append(['KeyError'])
        else:
            user_location.append([])

        return user_location

    def get_hashtags(self, t):
        hashtags = []
        if 'extended_tweet' in t:
            for hashtag in t['extended_tweet']['entities']['hashtags']:
                hashtags.append(hashtag['text'])
        elif 'hashtags' in t['entities'] and len(t['entities']['hashtags']) > 0:
            hashtags = [item['text'] for item in t['entities']['hashtags']]
        else:
            hashtags = []
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

        '''collect, filter and parse the tweets from twitter API'''

        t = json.loads(data)

        #only consider tweets that are non-retweets, non-replies, and non-quotes.
        if 'RT' not in t['text'] and t['in_reply_to_status_id'] == None and t['is_quote_status'] == False:

            tweet = self.get_tweet_dict(t)
            self.callback(tweet)
            #apply the callback to every single tweet data structure (i.e. the dictionary)
            #in this script, the callback is print(),
            #but in the greater program, the callback is db_interface.Load_DB.new_tweet()


            #can comment this out later. output_filename is mostly just for testing.
            with open(self.output_filename, 'a') as file:
                file.write(str(tweet))
            self.counter += 1

            #end stream once it hits a pre-defined limit.
            if self.counter == self.limit:
                return False

class TwitterStreamer():
    """Class containing the primary method / functionality of the script"""

    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator()

    def stream_tweets(self, limit, callback):
        listener = TwitterListener(limit, callback, OUTPUT_FILENAME)
        auth = self.twitter_authenticator.authenticate()
        stream = Stream(auth, listener)
        stream.filter(track=KEYWORDS_LIST, languages=LANGUAGES)


if __name__ == "__main__":

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(5, print)
