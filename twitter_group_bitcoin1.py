import socket
import sys
import requests
import requests_oauthlib
import json 
from kafka import KafkaProducer

apiKey = 'a3dcf7e7c492ad70f39d38b1014bf914a9074f614d7063ee0b89dcad92c1df11'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: x.encode('utf-8'))

def get_bitcoin():
    url = "https://min-api.cryptocompare.com/data/price"

    payload = {
        "api_key": apiKey,
        "fsym": "BTC",
        "tsyms": "USD"
    }

    result = requests.get(url, params=payload).json()
    print(result)
    
    print('----------------')
    
    print(result['USD'])
    
    return result#['USD']


def send_bitcoin_to_kafka(http_resp):
    for price in http_resp.values():
        try:
            print(price)
            print('-------------------')
            #print('try')
            #full_price = json.dumps(http_resp)
            #print(full_price)
            #full_price_json = json.loads(full_price)
            #print(full_price_json)
            #price_text = full_price_json['USD']
            #http_resp[list(http_resp.keys())[0]]
            #price_text = http_resp
            price_text = str(price)
            print("Price: " + price_text)
            print("------------------------------------------")
            #tcp_connection.send(bytes(tweet_text + '\n', 'utf-8'))
            #tcp_connection.send(price_text.encode('utf-8'))
            #tcp_connection.send(str(price_text))
            print('about to send')
            producer.send('bitcoin_vals', price_text)
            print('sent')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


while True:
    resp = get_bitcoin()
    send_bitcoin_to_kafka(resp)