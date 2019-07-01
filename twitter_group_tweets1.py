
from kafka import KafkaProducer

import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '1136165778921852933-rdqBR1V6H8Pa2UdwPzwpiATxC96xTg'
ACCESS_SECRET = '0zDv6YEHPtSN3Jz1v4xaa20DFngHYnwM47lvSTdsl4xmj'
CONSUMER_KEY = '0lh8bMvH1BB4HXUdyKmQpH93W'
CONSUMER_SECRET = 'u7kVYtltNE7w9fjWEVAkVc2KQJa9O7axEnYNsNPEJkJEzLiWnX'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','bitcoin')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            producer.send('bitcoin_tweets', tweet_text)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            
resp = get_tweets()
send_tweets_to_spark(resp)
            