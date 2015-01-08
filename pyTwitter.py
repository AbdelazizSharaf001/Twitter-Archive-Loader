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
import threading

from pprint import pprint



def get_inner_filenames(tar, tar_filename, attempt_from_db=True):
    """ Wrapper to try and retrieve saved file names for a specific TAR file instead of determining it every time.
    :param tar:
    :return:
    """
    if attempt_from_db:
        inner_files_txt = db['tar_file_names'].find_one(tar_filename=tar_filename)
        if inner_files_txt is not None:
            inner_files = inner_files_txt['tar_files_inside'].split("; ")
            print('Loaded filenames from the database...')
            return inner_files

    # Can't find it in DB, returning
    print('Loading names from tar file:' + tar_filename[:6] + '...' + tar_filename[-20:])
    inner_files = [filename for filename in tar.getnames() if filename.endswith('.bz2')]
    data = {'tar_filename': tar_filename,
            'tar_files_inside': "; ".join(inner_files)}
    print(data)
    print('Loaded filenames from the file...')
    db['tar_file_names'].upsert(data, ['tar_filename'])
    print('Saved filenames to the database for next time')
    return inner_files

# get_inner_filenames(tar, filename)

def scrape_tar_contents(filename):
    tar = tarfile.open(filename, 'r')
    tar_bz2_names = get_inner_filenames(tar, filename, True)

    num_bz2_files = len(tar_bz2_names)
    bz2_count = 1
    print('Starting work on file... ' + filename[-20:])
    for bz2_filename in tar_bz2_names:
        print('Starting work on inner file... ' + bz2_filename[-20:] + ': ' + str(bz2_count) + '/' + str(num_bz2_files))
        t_extract = tar.extractfile(bz2_filename)
        data = t_extract.read()
        current_line = 1
        try:
            txt = bz2.decompress(data)
        except IOError:
            error_log = {'Error_stage': 'BZ2 File Input',
                         'Date_time': datetime.datetime.now(),
                         'File_TAR': filename,
                         'File_BZ2': bz2_filename,
                         'Line_number': current_line,
                         'Error': str(e)}
            db['error_log'].upsert(error_log, ['File_TAR', 'File_BZ2', 'Line_number'])

        tweet_errors = 0
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

    filename = r'H:/Twitter datastream/Sourcefiles/archiveteam-twitter-stream-2013-01.tar'
    scrape_tar_contents(filename)