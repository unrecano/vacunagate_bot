import argparse
import csv
import logging
import os
import shutil
import tempfile
import time
import tweepy
from urllib import request
from dotenv import load_dotenv
from pymongo import MongoClient

# Read .env file.
load_dotenv(verbose=True)

# Configure log.
logging.basicConfig(level=logging.INFO)
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

# Auth Twitter.
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth, wait_on_rate_limit=True)

def search_words_on_twitter(text):
    text_query = text + " -filter:retweets"
    return tweepy.Cursor(api.search, q=text_query).items()

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
            retweet_and_favorite_a_tweet(tweet)
            logger.info(f"> {obj['N']} - {obj['user_scree_name']}, {obj['text']}")
            array.append(obj)
        except tweepy.TweepError as error:
            logger.error(str(error))
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
    collection.insert_many(tweets)
    logger.info("> Finished.")

def save_tweet(tweet):
    logger.info("> Starting save tweet.")
    collection = db.tweets
    logger.info("> Saving into database")
    collection.insert_one(tweet)
    logger.info("> Finished.")


def save_persons():
    logger.info("> Starting save persons.")
    collection = db.persons
    persons = []
    url = "https://raw.githubusercontent.com/RoTorresT/VacunaGate_Peru/main/487vacunados.csv"
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
    
    logger.info("> Saving into database")
    collection.insert_many(persons)
    logger.info("> Finished.")

# Create Listener.
class VacunagatesListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        self.me = api.me()
    
    def on_status(self, tweet):
        logger.info(f"> Processing tweet: {tweet.id}")
        if tweet.in_reply_to_status_id is not None or \
            tweet.user.id == self.me.id:

            return
        
        if not tweet.favorited:
            try:
                tweet.favorite()
            except Exception as e:
                logger.error("> Error on favorite", exc_info=True)
        
        if not tweet.retweeted:
            try:
                tweet.retweet()
            except Exception as e:
                logger.error("> Error on retweet", exc_info=True)
        
        parse = parse_tweet(tweet)
        save_tweet(parse)
    
    def on_error(self, status):
        logger.error(status)

def run_listener(keywords):
    logger.info("> Running Listener.")
    listener = VacunagatesListener(api)
    stream = tweepy.Stream(api.auth, listener)
    stream.filter(track=keywords)
    logger.info("> Watch for tweets.")

def post_persons():
    logger.info("> Starting post persons.")
    collection = db.persons
    for element in collection.find():
        text = f"{element['first_name']} {element['last_name']} de {element['age']} años de edad, VACUNADO. Observción: {element['observation']}, Proyecto: {element['project']}."
        try:
            api.update_status(text[:280])
            logger.info(f"> {text}")
            time.sleep(30)
        except tweepy.TweepError as error:
            logger.error(str(error))

if __name__ == '__main__':
    keywords = ["vacunagate", "vacunasgate"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", help="Save all data.", action="store_true")
    parser.add_argument("--stream", help="Listener tweets.", action="store_true")
    parser.add_argument("--tweet", help="Tweet names.", action="store_true")
    args = parser.parse_args()
    if args.save:
        save_persons()
        save_tweets(" ".join(keywords))
    elif args.stream:
        run_listener(keywords)
    elif args.tweet:
        post_persons()
    