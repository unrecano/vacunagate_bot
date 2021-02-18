import argparse
import csv
import datetime
import json
import logging
import os
import shutil
import tempfile
import time
import tweepy
from urllib import request
from urllib3.exceptions import ProtocolError
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, UpdateOne

# Read .env file.
load_dotenv(verbose=True)

# Configure log.
custom_format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
logging.basicConfig(level=logging.INFO, format=custom_format)
logger = logging.getLogger()

# Twitter keys.
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')

# Mongo keys.
mongo_user = os.getenv('MONGO_USER')
mongo_pass = os.getenv('MONGO_PASS')
mongo_host = os.getenv('MONGO_HOST')
mongo_uri = f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_host}/"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client.vacunagates
db.tweets.create_index([('id', ASCENDING)], unique=True)
db.retweets.create_index([('id', ASCENDING)], unique=True)

# Auth Twitter.
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

# Get twitter's profiles.
profiles = [p.strip() for p in os.getenv('PROFILES').split(',')]

def search_words_on_twitter(text):
    since = datetime.datetime.today().strftime("%Y-%m-%d")
    text_query = text + " -filter:retweets"
    return tweepy.Cursor(api.search, q=text_query, since=since).items()

def parse_tweet(tweet):
    return {
        "id": tweet.id,
        "user_name": tweet.user.name,
        "user_location": tweet.user.location,
        "user_scree_name": tweet.user.screen_name,
        "text": tweet.text,
        "created_at": str(tweet.created_at),
        "geo": tweet.geo,
        "favorited": tweet.favorited,
        "retweeted": tweet.retweeted,
    }

def retweet_and_favorite_a_tweet(tweet):
    if not tweet.favorited:
        tweet.favorite()
    
    if not tweet.retweeted:
        tweet.retweet()

def get_all_tweets(text):
    array = []
    tweets = search_words_on_twitter(text)
    for tweet in tweets:
        try:
            obj = parse_tweet(tweet)
            if tweet.user.screen_name in profiles:
                retweet_and_favorite_a_tweet(tweet)
                save_retweet(obj)
            logger.info(f"> {obj['id']} - {obj['user_scree_name']}, {obj['text']}")
            array.append(obj)
        except tweepy.TweepError as error:
            logger.error(str(error))
            if "'code': 419" in error.reason:
                time.sleep(900)
                continue
        except StopIteration:
            break
    return array

def save_tweets(text):
    logger.info("> Starting save tweets.")
    collection = db.tweets
    tweets = get_all_tweets(text)
    logger.info("> Saving into database")
    l = [UpdateOne({'id': t['id']}, {'$set': t}, upsert=True) for t in tweets]
    collection.bulk_write(l)
    logger.info("> Finished.")

def save_retweet(tweet):
    logger.info("> Starting save retweet.")
    collection = db.retweets
    logger.info("> Saving into database")
    collection.update_one({'id': tweet['id']}, {'$set': tweet}, upsert=True)
    logger.info("> Finished.")

def get_all_persons():
    logger.info("> Getting all persons.")
    collection = db.persons
    persons = []
    url = "https://raw.githubusercontent.com/unrecano/VacunaGate_Peru/main/487vacunados.csv"
    content = request.urlopen(url)
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            shutil.copyfileobj(content, tmp_file)

    line = 0
    with open(tmp_file.name) as csv_file:
        headers = ('N', 'place', 'last_name', 'first_name', 'age', 'dni',
            'date_1', 'date_2', 'date_3', 'observation', 'project')
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            if line > 0:
                obj = dict(zip(headers, row))
                logger.info(f"> {obj['N']} - {obj['last_name']}, {obj['first_name']}")
                persons.append(obj)
            line += 1
    return persons

def save_persons():
    logger.info("> Starting save persons.")
    collection = db.persons
    persons = get_all_persons()
    logger.info("> Saving into database")
    l = [UpdateOne({'N': p['N']}, {'$set': p}, upsert=True) for p in persons]
    collection.bulk_write(l)
    logger.info("> Finished.")

# Create Listener.
class VacunagatesListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        self.me = api.me()
    
    def on_status(self, tweet):
        if tweet.in_reply_to_status_id is not None or \
            tweet.user.id == self.me.id:

            return
        
        if tweet.user.screen_name in profiles:
            logger.info(f"> Processing tweet from {tweet.user.screen_name}")
            if not tweet.favorited:
                try:
                    tweet.favorite()
                except Exception as error:
                    logger.error(str(error))
            
            if not tweet.retweeted:
                try:
                    tweet.retweet()
                except Exception as error:
                    logger.error(str(error))
        
            parse = parse_tweet(tweet)
            save_retweet(parse)
    
    def on_error(self, status):
        logger.error(status)

def run_listener(keywords):
    logger.info("> Running Listener.")
    listener = VacunagatesListener(api)
    stream = tweepy.Stream(api.auth, listener)
    while True:
        try:
            stream.filter(track=keywords)
        except (ProtocolError, AttributeError) as error:
            logger.error(str(error))
            continue
    logger.info("> Watch for tweets.")

def post_persons():
    logger.info("> Starting post persons.")
    collection = db.persons
    for element in collection.find():
        text = f"{element['first_name']} {element['last_name']} de {element['age']} años de edad, VACUNADO. Observción: {element['observation']}, Proyecto: {element['project']}."
        try:
            api.update_status(text[:280])
            logger.info(f"> {text}")
            time.sleep(5)
        except tweepy.TweepError as error:
            logger.error(str(error))

def save_persons_test():
    logger.info("> Starting save persons.")
    persons = get_all_persons()
    logger.info("> Saving into database")
    with open('persons.json', mode='w') as f:
        json.dump(persons, f, indent=2)
    logger.info("> Finished.")

def save_tweets_test(text):
    logger.info("> Starting save tweets.")
    tweets = get_all_tweets(text)
    logger.info("> Saving into database")
    with open('tweets.json', mode='w') as f:
        json.dump(tweets, f, indent=2)
    logger.info("> Finished.")

if __name__ == '__main__':
    keywords = [f"#{w.strip()}" for w in os.getenv('HASHTAGS').split(',')]
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", help="Save all data.", action="store_true")
    parser.add_argument("--stream", help="Listener tweets.", action="store_true")
    parser.add_argument("--tweet", help="Tweet names.", action="store_true")
    parser.add_argument("--test_save", help="Test.", action="store_true")

    args = parser.parse_args()
    if args.test_save:
        save_persons_test()
        for word in keywords:
            save_tweets_test(word)
    if args.save:
        save_persons()
        for word in keywords:
            save_tweets(word)
    elif args.stream:
        run_listener(keywords)
    elif args.tweet:
        post_persons()