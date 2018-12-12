import argparse
import pymongo
from twitter_streamer import TwitterStreamer
import config


class Load_DB:

    def __init__(self, batch_size, limit):
        self.batch_size = batch_size
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

        #logic for handling if batch size > limit and if limit % batch_size != 0
        if self.limit - self.counter < self.batch_size:
            self.batch_size = self.limit - self.counter

        if len(self.buffer) >= self.batch_size:
            self.load_tweets()
            print(f"loaded {int(self.counter + self.batch_size)} tweets of {int(self.limit)}")
            self.buffer = []
            self.counter += self.batch_size

def populate_database(batch_size, limit):
    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(limit, Load_DB(batch_size, limit).collect_tweets)
    #Load_DB.collect_tweets is the callback.

if __name__ == '__main__':

    """
    To view argument parser help in the command line:
    'python load_database.py -h'
    """

    parser=argparse.ArgumentParser(description='Collect tweets and put them into a database')
    parser.add_argument('-b', '--batch_size', type=int, default=2, help='How many tweets do you want to grab at a time?')
    # parser.add_argument('--client', type=str, default="", help='Enter the string that points to the relevant database')
    parser.add_argument('-n', '--total_number', type=int, default=10, help='How many total tweets do you want to get?')

    args = parser.parse_args()
    # if args.client == "":
    #     client = pymongo.MongoClient()
    # else:
    #     client = pymongo.MongoClient(args.client)

    print("loading data to database...\n")
    populate_database(args.batch_size, args.total_number)
