
from twarc import Twarc
import pandas as pd
import os
import json

consumer_key = '2bRZ97vh62E1qaJOPNYGo49pw'
consumer_secret = 'z0KUPkm745VyGBtYR2Txu1QC4UVbeW5l88iSUS0T3xif5giEpX'
access_token = '1543851236377632769-ExZT0CO1FXp8P4mCbS3FmAmMjfspm4'
access_token_secret = 'BwFiefnOiNdww1qhMBA39er2iayrchib9Sa3IeAdvJChe'

t_inst = Twarc(consumer_key,consumer_secret,access_token, access_token_secret)

num_tweets=0
test = 0
# change this to different month such as 'tweets_ids/.../2020-10'

folder = 'temp_tweet_id/2020-10/'
for filename in os.listdir(folder):
    hour = filename[-6:-4]
    day = filename[-9:-7]
    month = filename[-12:-10]
    if day == '23':
        f = os.path.join(folder, filename)
        print(f)
        data = []
        for tweet in t_inst.hydrate(open(f)):
            num_tweets += 1
            # skips tweet if it doesn't have any location info 
            if tweet['place'] is None and tweet['user']['location'] == '':
                continue
            if tweet['place'] is None:
                place = None
                country = None
            else:
                place = tweet['place']['full_name']
                country = tweet['place']['country']
    
            data.append([tweet['id'], tweet['full_text'], tweet['user']['location'],place,country,tweet['retweeted'],tweet['retweet_count'],tweet['favorited'],tweet['favorite_count'],tweet['lang'],f'{month}-{day}-{hour}',tweet['user']['verified']])
            if num_tweets % 5000 == 0:
                print('Number of tweets hydrated:{}'.format(num_tweets))
               
        
        tweetdf = pd.DataFrame(data,columns=['id', 'text', 'location','place','country','retweeted','retweet_count','favorited','favorite_count','language','date','verified'])
        tweetdf.to_csv(f'hydrated_tweets/2020-{month}/df{month}-{day}-{hour}.csv',sep=';',index=False,header=True,encoding='utf-8')
