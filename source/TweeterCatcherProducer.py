#TODO Arquivo unutilizado estrategia alterada.
import tweepy
from kafka import KafkaProducer
from constants import *
from json import dumps


def run():
    auth = tweepy.OAuth1UserHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda K: dumps(K).encode('utf-8'))
    api = tweepy.API(auth)
    tweets = tweepy.Cursor(api.search_tweets, q="#santander", tweet_mode='extended', lang="pt").items(100)

    for tweet in tweets:
        producer.send('SantanderTweets', tweet)


if __name__ == '__main__':
    run()
