"""
Read the output of a zipped twitter archive from:
https://archive.org/details/twitterstream
"""
import bz2
import datetime
import json
import os
import profile
import psycopg2

from pprint import pprint


with open("postgresConnecString.txt", 'r') as f:
    DB_CONNECTIONSTRING = f.readline()

conn = psycopg2.connect(DB_CONNECTIONSTRING)
CACHE_DIR = "H:/Twitter datastream/PYTHONCACHE"

def load_bz2_json(filename):
    """ Takes a bz2 filename, returns the tweets as a list of tweet dictionaries"""
    with open(filename, 'rb') as f:
        s = f.read()
        lines = bz2.decompress(s).split("\n")
    num_lines = len(lines)
    tweets = []
    for line in lines:
        try:
            if line == "":
                num_lines -= 1
                continue
            tweets.append(json.loads(line))
        except: # I'm kind of lenient as I have millions of tweets, most errors were due to encoding or so)
            continue
    return tweets


def load_tweet(tweet, tweets_saved):
    """Takes a tweet (dictionary) and upserts its contents to a PostgreSQL database"""
    try:
        tweet_id = tweet['id']
        tweet_text = tweet['text']
        tweet_locale = tweet['lang']
        created_at = tweet['created_at']
    except KeyError:
        return tweets_saved

    data = {'tweet_id': tweet_id,
            'tweet_text': tweet_text,
            'tweet_locale': tweet_locale,
            'created_at_str': created_at,
            'date_loaded': datetime.datetime.now(),
            'tweet_json': json.dumps(tweet)}
    cur = conn.cursor()
    try:
        cur.execute("""INSERT INTO tweets_test (tweet_id, tweet_text, tweet_locale, created_at_str, date_loaded, tweet_json)
                       VALUES (%s, %s, %s, %s, %s, %s);""", (data['tweet_id'], data['tweet_text'], data['tweet_locale'],
                                                             data['created_at_str'], data['date_loaded'], data['tweet_json']))
    except: # Kind of lenient for errors, here again.
        return tweets_saved
    finally:
        cur.close()
    tweets_saved += 1
    return tweets_saved


def handle_file(filename, retry=False):
    """Takes a filename, loads all tweets into a PostgreSQL database"""
    tweets = load_bz2_json(filename)
    tweets_saved = 0
    for tweet in tweets:
        tweets_saved = load_tweet(tweet, tweets_saved)  # Extracts proper items and places them in database
    conn.commit()
    return True

def main():
    files_processed = 0
    for root, dirs, files in os.walk(CACHE_DIR):
        for file in files:
            files_processed +=1
            filename = os.path.join(root, file)
            #print(file)
            print('Starting work on file ' + str(files_processed) + '): ' + filename)
            handle_file(filename)
            if files_processed == 1000:
                break
        if files_processed == 1000:
            break

if __name__ == "__main__":
    pprint('Starting work!')
    profile.run('main()')
    conn.close()
else:  # If running interactively in interpreter (Pycharm):
    filename = r"H:\Twitter datastream\PYTHONCACHE\2013\01\01\00\00.json.bz2"
