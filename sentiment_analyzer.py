from geocoder import main
from geocoder import STARTTIME, NUM_DOCS
import re
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime #for testing time of script execution

RE_URLS = 'http[s]?:\/\/(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+'
RE_AT_MENTIONS = '(?:@[\w_]+)'
RE_HASHTAGS = '#'
RE_EXTRA_WHITE_SPACE = '\s+'
RE_INSIDE_PARENTHESIS = '\([^)]*\)'
RE_SPECIAL_CHARS = "\.|\,|\\|\r|\n|\s|\(|\)|\"|\[|\]|\{|\}|\;|\:|\.|\°|\-|\/|\&|\(|\)|\||\*"
#preserve question marks and exclamation marks for Vader

EMOJI_PATTERN = re.compile("["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                       "]+", flags=re.UNICODE)

EMOJI_PATTERN2 = re.compile(u'('
    u'\ud83c[\udf00-\udfff]|'
    u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
    u'[\u2600-\u26FF\u2700-\u27BF])+',
    re.UNICODE)

class CleanText():

    """
       Clean the text of the tweet as well as the user description (for later analysis).
       Preserving things like emojis and exclamation marks for Vader Sentiment Analyzer,
       since it is able to interpret meaning / emotional value from these symbols.
       """

    def clean_tweet(self, text):
        text = re.sub(RE_URLS, " ", str(text))
        text = re.sub(RE_AT_MENTIONS, " ", text)
        text = re.sub(RE_HASHTAGS," ", text)
        text = re.sub(RE_SPECIAL_CHARS," ",text)
        text = text.strip()
        text = re.sub(RE_EXTRA_WHITE_SPACE, " ", text)

        return text

    def clean_user(self, text):
        # print(f'\n\nemoji pattern type: {type(EMOJI_PATTERN)}\n\n')
        text = re.sub(RE_URLS, " ", str(text))
        text = re.sub(RE_AT_MENTIONS, " ", text)
        text = re.sub(RE_INSIDE_PARENTHESIS, " ", text)
        text = re.sub(RE_HASHTAGS," ", text)
        text = re.sub(EMOJI_PATTERN, " ", text)
        text = re.sub(EMOJI_PATTERN2, " ", text)
        text = re.sub(RE_SPECIAL_CHARS," ",text)
        text = text.strip()
        text = re.sub(RE_EXTRA_WHITE_SPACE, " ", text)
        return text


class Analyzer():

    def __init__(self, df):
        self.df = df

    def analyze_sentiment(self):

        self.df['text'] = self.df['text'].apply(CleanText().clean_tweet)
        self.df['user_description'] = self.df['user_description'].apply(CleanText().clean_user)

        all_tweets = list(self.df['text'])

        analyzer = SentimentIntensityAnalyzer()

        """
        Can also include sentiment 'sub-scores' (i.e. negative, neutral, and positive),
        but for now only including composite sentiment. Others are commented out.
        """
        # neg_sent = []
        # neu_sent = []
        # pos_sent = []
        comp_sent = []

        for tw in all_tweets:

            vs = analyzer.polarity_scores(tw)
            # neg_sent.append(vs['neg'])
            # neu_sent.append(vs['neu'])
            # pos_sent.append(vs['pos'])
            comp_sent.append(vs['compound'])

        # self.df['neg. sentiment'] = neg_sent
        # self.df['neu. sentiment'] = neu_sent
        # self.df['pos. sentiment'] = pos_sent
        self.df['comp. sentiment'] = comp_sent

        self.df['strong positive'] = self.df['comp. sentiment'].map(lambda x: 1 if x >= 0.8 else 0)
        self.df['strong negative'] = self.df['comp. sentiment'].map(lambda x: 1 if x <= -0.8 else 0)


        """CONSIDER FILTERING OUT SENTIMENTS THAT FALL WITHIN 'MIDDLE RANGE'
           (e.g. anything between -0.5 -- 0.5 ) """

        """Will eventually return the resulting df, but for now printing it to csv for testing"""

        test_filename = 'sandbox/live_demo.csv'
        if os.path.exists(test_filename):
            print(f'\n\n{test_filename} already exists; removing it first\n')
            os.remove(test_filename)

        with open(test_filename, 'w') as f:
            self.df.to_csv(f, header=True)
        print("Successfully printed to csv!\n\n")

        return self.df

if __name__ == '__main__':

    print(f"\n\nNumber of documents currently in DB: {NUM_DOCS}\n")
    df = main()
    print(f'Passing dataframe to the sentiment analyzer...')
    sentiment_analyzer = Analyzer(df)
    sentiment_analyzer.analyze_sentiment()
    print(f"Time to completion: {datetime.now() - STARTTIME}")
