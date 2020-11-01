#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 00:19:01 2020

@author: joe phoenix aka dogtime
"""

from __future__ import print_function
import tweepy
import psycopg2
import psycopg2.extras
from configscraper import access_token_key, access_token_secret, consumer_key, \
    consumer_secret, user, host, port, database, password
    
    

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=False)
connection = psycopg2.connect(user = user, password = password, host = host, port = port, dbname = database)
cursor = connection.cursor()


def rat_listing(oldest_tweetid):
    print(oldest_tweetid)
    tweets = []          
    if oldest_tweetid is None:
        tweets = tweepy.Cursor(api.search,q="#election2020",count=50,
                           lang="en", tweet_mode='extended').items(100)
    else:
        tweets = tweepy.Cursor(api.search,q="#election2020",count=50,
                           lang="en", tweet_mode='extended', max_id=oldest_tweetid).items(100)
    print("I made it past twitter")
    print(tweets)
    twet = filter(lambda tweet: (not tweet.retweeted) and ('RT @' not in tweet.full_text), tweets)
    texttweet = list(map(lambda tweet: {'text': tweet.full_text, 'date': tweet.created_at, 'dirlink': tweet.id_str}, twet))
    print(len(texttweet))
    return texttweet
    
def oldest_rat():
    postgres_search_query = "SELECT dirlink from ratscratch order by tweetdate limit 1"
    cursor.execute(postgres_search_query)
    oldest = cursor.fetchone()
    if oldest is not None:
        return oldest[0]
    else:
        return None

def rat_sql(tweets_to_save):    
    try:
        postgres_insert_query = """ INSERT INTO ratscratch (tweetdate, tweettext, dirlink) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING"""
        for tweet in tweets_to_save:
            cursor.execute(postgres_insert_query, (tweet['date'],tweet['text'],tweet['dirlink']))
        connection.commit()
    except (Exception, psycopg2.Error) as err:
        if(connection):
            print("failed to insert into ratscratch", err)
        print("error", err)


oldest_tweetid = oldest_rat()
tweets_to_save = rat_listing(oldest_tweetid)   
rat_sql(tweets_to_save)

