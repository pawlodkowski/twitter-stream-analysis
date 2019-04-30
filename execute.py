from geocoder import main
from sentiment_analyzer import Analyzer
from load_database import populate_database
from twitter_streamer import TwitterStreamer, STARTTIME
from datetime import datetime #for testing time of script execution
import argparse

import pandas as pd
import re
import folium
import json
import branca
from sklearn.preprocessing import MinMaxScaler
from math import log
import time
from collections import Counter
import webbrowser
import config


LOCAL_DB = config.LOCAL_CLIENT.tweets
HTML_PATH = 'geojson/testTwitterAnalysisMap.html'
CHROME_PATH = 'open -a /Applications/Google\ Chrome.app %s'

NUM_DOCS = LOCAL_DB.tweet_dicts.count_documents({"id":{"$exists":True}})
DEMO_FILE = 'sandbox/live_demo.csv' ### FOR DEMONSTRATION
MAP_FILE = 'geojson/TwitterAnalysisMap.json'
GJ = open(MAP_FILE, mode='r').read()
TWITTER_GEOJSON = json.loads(GJ)

DF = pd.read_csv(DEMO_FILE)
DF = DF.fillna('')

# class Visualization():
#
# def __init__(self):
#     DF = df

def get_top_hashtags(list_of_strings):
    c = Counter(list_of_strings)
    return c.most_common(3)

def get_top_tweets(list_of_tweets):
    pass

DF['text_wrapped'] = DF['text'].map(lambda x: [x] if x else [])
DF['hashtags_wrapped'] = DF['hashtags'].map(lambda x: [x] if x else [])

### GROUP-BYS
geo_lats = DF.groupby(['GeoRegion'])[['GeoRegion_RP_Lat']].first().reset_index()
geo_lons = DF.groupby(['GeoRegion'])[['GeoRegion_RP_Lon']].first().reset_index()
geo_areas = DF.groupby(['GeoRegion'])[['GeoRegion_Area']].first().reset_index()
sent_avg = DF.groupby(['GeoRegion'])[['comp. sentiment']].mean().reset_index()
sent_count = DF.groupby(['GeoRegion'])[['comp. sentiment']].count().reset_index()
sent_count_strpos = DF.groupby(['GeoRegion'])[['strong positive']].sum().reset_index()
sent_count_strneg = DF.groupby(['GeoRegion'])[['strong negative']].sum().reset_index()

top_hashtags = DF.groupby(['GeoRegion'])[['hashtags_wrapped']].sum().reset_index()
top_hashtags['top_hashtags'] = top_hashtags['hashtags_wrapped'].apply(get_top_hashtags)

### Scaling sentiment counts to better conform to varying opacity of polygons in map.

sent_count['comp. sentiment scaled'] = sent_count['comp. sentiment'].map(lambda x: log(x) if x >= 1 else None)
scaler = MinMaxScaler(copy=True, feature_range = (0.4, 1.0))
X = sent_count['comp. sentiment scaled'].values.reshape(-1, 1)
scaler.fit(X)
sent_count['comp. sentiment scaled'] = scaler.transform(X)

### CONVERT GROUP-BYS TO SERIES
geo_lats_series = geo_lats.set_index('GeoRegion')['GeoRegion_RP_Lat']
geo_lons_series = geo_lons.set_index('GeoRegion')['GeoRegion_RP_Lon']
geo_areas_series = geo_areas.set_index('GeoRegion')['GeoRegion_Area']
sent_series = sent_avg.set_index('GeoRegion')['comp. sentiment']
count_series = sent_count.set_index('GeoRegion')['comp. sentiment']
count_series_scaled = sent_count.set_index('GeoRegion')['comp. sentiment scaled']
strpos_series = sent_count_strpos.set_index('GeoRegion')['strong positive']
strneg_series = sent_count_strneg.set_index('GeoRegion')['strong negative']
top_hashtags_series = top_hashtags.set_index('GeoRegion')['top_hashtags']

### MARKERS
marker_region = list(sent_series.index)
marker_lat = list(geo_lats_series)
marker_lon = list(geo_lons_series)
marker_area = list(geo_areas_series)
marker_avg_sent = list(sent_series)
marker_count = list(count_series)
marker_hashtags = list(top_hashtags_series)

assert len(marker_region) == len(marker_lat) == len(marker_lon) == len(marker_area) == len(marker_avg_sent) == len(marker_count) == len(marker_hashtags)

sent_colorscale = branca.colormap.linear.RdYlGn_09.scale(-1, 1)
sent_colorscale.caption = 'Avg. Sentiment Value of Tweets from Geographic Region'

def sent_style(feature):
    sentiment = sent_series.get(str(feature['properties']['name']), None)
    count_scaled = count_series_scaled.get(str(feature['properties']['name']), None)

    return {
        'fillOpacity': count_scaled,
        'weight': 0.1,
        'fillColor': '#ededed' if sentiment is None else sent_colorscale(sentiment)
    }

def sent_highlight(feature):
    sentiment = sent_series.get(str(feature['properties']['name']), None)
    return {
        'fillColor': '#ededed' if sentiment is None else sent_colorscale(sentiment),
        'fillOpacity' : 0.85,
        'weight': 1,
    }

def plot_html():
    tweetmap = folium.Map(location=[52.54, 13.36],
                zoom_start=4.25,
                tiles='CartoDB positron')

    geoSentiment = folium.GeoJson(TWITTER_GEOJSON, #the actual data / json tile
                                  style_function=sent_style,
                                  highlight_function=sent_highlight,
                                  name = 'SentimentAnalysis',
                                  control=True,
                                  tooltip=folium.GeoJsonTooltip(fields=['name'],
    #                                   style="width:20px;height:20px;background-color:#ffcc00"
                                     )
                                )

    markers = folium.FeatureGroup(name="GeoRegionMarkers")

    # Loop through data to plot markers
    for mr, lat, lon, area, sent, ct, ht in zip(marker_region,
                                                marker_lat,
                                                marker_lon,
                                                marker_area,
                                                marker_avg_sent,
                                                marker_count,
                                                marker_hashtags):

        df = pd.DataFrame(data=[ [mr], [sent], [ct], [ht] ],
                          columns=['value'],
                          index = ['Region',
                                   'Avg. Sent',
                                   'Tweet Count',
                                   'Top Hashtags'
                                    ])
        html = df.to_html(classes='table table-striped table-hover table-condensed table-responsive')

        markers.add_child(folium.Circle(location=[lat, lon],
                                              radius = area*100,
                                              popup=folium.Popup(html),
    #                                           tooltip=str(f'avg. sent: {sent}')+ " --",
                                              fill_color='#ededed' if sent is None else sent_colorscale(sent),
                                              fill=True,
                                              color = 'grey',
                                              fill_opacity=0.7
                                             )
                         )

    # add the layers
    sent_colorscale.add_to(tweetmap)
    geoSentiment.add_to(tweetmap)
    tweetmap.add_child(markers)
    tweetmap.add_child(folium.LayerControl())

    return tweetmap

if __name__ == '__main__':

    """
    To view argument parser help in the command line:
    'python execute.py -h'
    """

    parser=argparse.ArgumentParser(description='Collect, store, analyze, and visualize tweets in (near) real-time.')

    parser.add_argument('-k','--keyword_list', nargs='+', help='<Required> Enter any keywords (separated by spaces; no punctuation) that should be included in streamed tweets.', required=True)
    parser.add_argument('-b', '--batch_size', type=int, default=10, help='How many tweets do you want to grab at a time?')
    parser.add_argument('-n', '--total_number', type=int, default=100, help='How many total tweets do you want to get?')


    args = parser.parse_args()

    print("\n\nPhase 1: Scraping tweets and loading data to database...\n")
    populate_database(args.batch_size, args.total_number, args.keyword_list)

    print(f"\n\nProceeding to Phase 2: Extracting and analyzing data...\n\n")
    time.sleep(5)
    print(f"Number of documents currently in DB: {NUM_DOCS}\n")
    input_df = main()
    sentiment_analyzer = Analyzer(input_df)
    output_df = sentiment_analyzer.analyze_sentiment()

    tweetmap = plot_html()
    tweetmap.save(HTML_PATH)
    print("\n\n\n\n\n-----SUCCESSFULLY PRODUCED MAP!-----\n\n\n\n\n")
    webbrowser.get(CHROME_PATH).open(HTML_PATH)
    webbrowser.get(CHROME_PATH).open(DEMO_FILE)



    print(f"Time to completion: {datetime.now() - STARTTIME}")
