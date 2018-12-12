import argparse
import pymongo
from twitter_streamer import TwitterStreamer
import config


class Load_DB:

    def __init__(self, chunk_size, limit):
        self.chunk_size = chunk_size
        #self.db = client
        self.buffer = []
        self.limit = limit
        self.counter = 0

    def load_tweets(self):
        '''insert the data into the mongoDB into a collection called test_collection.
        if test_collection doesn't exist, it will be created.'''


        # followers = [tweet['followers'] for tweet in self.buffer]
        # for tweet in self.buffer:
        #     if tweet['followers'] == max(followers):
        #         tweet['interesting'] = 1
        config.client.tweets.test_collection.insert(self.buffer)

    def collect_tweets(self, tweet):
        self.buffer.append(tweet)

        #logic for handling if chunk size > limit for whatever reason
        if self.limit - self.counter < self.chunk_size:
            self.chunk_size = self.limit - self.counter

        if len(self.buffer) >= self.chunk_size:
            self.load_tweets()
            self.buffer = []
            self.counter += self.chunk_size

def populate_database(chunk_size, limit):
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(limit, Load_DB(chunk_size, limit).collect_tweets)
    #get_tweets(limit, callback = read_into_Mongo.new_tweet)

if __name__ == '__main__':

    # python db_interface.py -h
    parser=argparse.ArgumentParser(description='Collect tweets and put them into a database')
    parser.add_argument('-c', '--chunk_size', type=int, default=2, help='How many tweets do you want to grab at a time?')
    # parser.add_argument('--client', type=str, default="", help='Enter the string that points to the relevant database')
    parser.add_argument('-n', '--total_number', type=int, default=10, help='How many total tweets do you want to get?')

    args = parser.parse_args()
    # if args.client == "":
    #     client = pymongo.MongoClient()
    # else:
    #     client = pymongo.MongoClient(args.client)

    populate_database(args.chunk_size, args.total_number)
