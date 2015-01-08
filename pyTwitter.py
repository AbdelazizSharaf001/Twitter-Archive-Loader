__author__ = 'Mathijs'
import bz2
import json
import dataset
import os
import datetime
import time
from time import sleep
from pprint import pprint


with open("postgresConnecString.txt", 'r') as f:
    DB_CONNECTIONSTRING = f.readline()

DB = dataset.connect(DB_CONNECTIONSTRING)
CACHE_DIR = "H:/Twitter datastream/PYTHONCACHE"

def load_bz2_json(filename):
    """ Takes a filename, extracts the tweets as a list of tweets

    :param filename: path to file.
    :return: list dictionaries, one for each tweet
    """
    with open(filename, 'rb') as f:
        s = f.read()
        lines = bz2.decompress(s).split("\n")
    num_lines = len(lines)
    tweets = []
    # line = lines[0]
    for line in lines:
        try:
            if line == "":
                num_lines -= 1
                continue
            tweets.append(json.loads(line))
        except:
            print(line)
    print(str(len(tweets)) + ' of ' + str(num_lines) + ' lines succesfully converted to tweets')
    return tweets

def extract_from_tweet(tweet, tweets_saved):
    try:
        tweet_id = tweet['id']
        tweet_text = tweet['text']
        tweet_locale = tweet['lang']
        created_at = tweet['created_at']
        tweet_json = tweet
        data = {'tweet_id': tweet_id,
                'tweet_text': tweet_text,
                'tweet_locale': tweet_locale,
                'created_at_str': created_at,
                'date_loaded': datetime.datetime.now(),
                'tweet_json': json.dumps(tweet_json)}
        DB['tweets'].upsert(data, ['tweet_id'])
        tweets_saved += 1
        if tweets_saved % 100 == 0:
            print('Saved ' + str(tweets_saved) + ' tweets')
        return tweets_saved
    except KeyError:
        return tweets_saved

def handle_file(filename):
    files_seen = [filename for filename in DB['tweet_log']]
    print('Loading tweets from file: ' + filename[34:])
    start = time.time()
    tweets = load_bz2_json(filename)
    time_elapsed = time.time() - start
    print('Succesfully loaded file and extracted ' + str(len(tweets)) + ' to list in ' + str(time_elapsed) + ' seconds')
    sleep(2)

    print('Going to save them to database now')
    start = time.time()
    #tweet = tweets[0]
    tweets_saved = 0
    for tweet in tweets:
        tweets_saved = extract_from_tweet(tweet, tweets_saved) # Extracts proper items and places them in database
    # Attempting to Thread now.




    time_elapsed = time.time() - start
    print('Succesfully saved ' + str(len(tweets)) + ' to db in ' + str(time_elapsed) + ' seconds')
    return True

def main():
    files_processed = 0
    for root, dirs, files in os.walk(CACHE_DIR):
        for file in files:
            files_processed +=1
            filename = os.path.join(root, file)
            handle_file(filename)
            metadata = {'last_seen': datetime.datetime.now(),
                        'filename': filename}
            DB['tweet_log'].upsert(metadata, ['filename'])
            if files_processed == 1:
                break
        if files_processed == 1:
            break



if __name__ == "__main__":
    pprint('Starting work!')
    main()
else:
    filename = r"H:\Twitter datastream\PYTHONCACHE\2013\01\01\00\00.json.bz2"