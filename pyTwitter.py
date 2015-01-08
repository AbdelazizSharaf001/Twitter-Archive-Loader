__author__ = 'Mathijs'
import bz2
import json
import dataset
import psycopg2
import os
import datetime
import tarfile
import tempfile
import sys
#from thready import threaded
from pprint import pprint

filename = r"H:\Twitter datastream\PYTHONCACHE\2013\01\01\00\00.json.bz2"
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

a = load_bz2_json(filename)



    bz2_count = 1
    print('Starting work on file... ' + filename[-20:])
    for bz2_filename in tar_bz2_names:
        print('Starting work on inner file... ' + bz2_filename[-20:] + ': ' + str(bz2_count) + '/' + str(num_bz2_files))
        t_extract = tar.extractfile(bz2_filename)
        data = t_extract.read()
        txt = bz2.decompress(data)
        bz2.BZ2File()

        tweet_errors = 0
        current_line = 1
        num_lines = len(txt.split('\n'))
        for line in txt.split('\n'):
            if current_line % 100 == 0:
                print('Working on line ' + str(current_line) + '/' + str(num_lines))
            try:
                if line == "":
                    continue
                try:
                    tweet = json.loads(line)
                except ValueError, e:
                    error_log = {'Date_time': datetime.datetime.now(),
                             'File_TAR': filename,
                             'File_BZ2': bz2_filename,
                             'Line_number': current_line,
                             'Line': line,
                             'Error': str(e)}
                    tweet_errors += 1
                    db['error_log'].upsert(error_log, ['File_TAR', 'File_BZ2', 'Line_number'])
                    print('Error occured, now at ' + str(tweet_errors))
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
                            'tweet_json': tweet_json}
                    db['tweets'].upsert(data, ['tweet_id'])
                except KeyError, e:
                    error_log = {'Date_time': datetime.datetime.now(),
                             'File_TAR': filename,
                             'File_BZ2': bz2_filename,
                             'Line_number': current_line,
                             'Line': line,
                             'Error': str(e)}
                    tweet_errors += 1
                    db['error_log'].upsert(error_log, ['File_TAR', 'File_BZ2', 'Line_number'])
                    print('Error occured, now at ' + str(tweet_errors))
                    continue
            except ValueError, e:
                error_log = {'Date_time': datetime.datetime.now(),
                             'File_TAR': filename,
                             'File_BZ2': bz2_filename,
                             'Line_number': current_line,
                             'Error': str(e)}
                tweet_errors += 1
                db['error_log'].upsert(error_log, ['File_TAR', 'File_BZ2', 'Line_number'])
                print('Error occured, now at ' + str(tweet_errors))
            current_line += 1
        bz2_count += 1
    print('Completed!')
    return 'Complete!'


if __name__ == "__main__":
    with open("postgresConnecString.txt", 'r') as f:
        db_connectionstring = f.readline()
    db = dataset.connect(db_connectionstring)
    CACHE_DIR = "H:/Twitter datastream/PYTHONCACHE"
    filename = r'H:/Twitter datastream/Sourcefiles/archiveteam-twitter-stream-2013-01.tar'
    scrape_tar_contents(filename)


    files_processed = 0
    for subdir, dirs, files in os.walk(CACHE_DIR):
        for file in files:
            files_processed += 1
            with open(file,'r') as f:
                lines = f.readlines()
            if files_processed == 2:
                break
        if files_processed == 2:
            break