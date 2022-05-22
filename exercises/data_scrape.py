#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# This notebook/script gets the Reddit feed, tabulates the data, and uploads it to a bucket

# Import libraries
import os
from google.cloud import storage
import feedparser
from bs4 import BeautifulSoup
from bs4.element import Comment
import json
import time


# In[ ]:


# Define values of project, bucket_name, and url
PROJECT='umc-dsa-8420-fs2021'
bucket_name = 'bmgwd9-final-bucket'
storage_client = storage.Client(project=PROJECT)
bucket = storage_client.get_bucket(bucket_name)
# Define URL of the RSS Feed I want
a_reddit_rss_url = 'http://www.reddit.com/new/.rss?sort=new'


# In[ ]:


# Define the necessary functions
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True
    
def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)
    
def create_json(json_object, filename):
    '''
    this function will create json object in
    google cloud storage
    '''
    # create a blob
    blob = bucket.blob(filename)
    # upload the blob 
    blob.upload_from_string(
        data=json.dumps(json_object),
        content_type='application/json'
        )
    result = filename + ' upload complete'
    return {'response' : result}


# In[ ]:


# Get feed
feed = feedparser.parse(a_reddit_rss_url)

# Define json object (dictionary) that will be populated with post data
json_object = {"timestamp":[], "title":[], "summary":[], "link":[]}

if (feed['bozo'] == 1):
    print("Error Reading/Parsing Feed XML Data")    
else:
    # add post data to the dictionary
    for item in feed[ "items" ]:
        dttm = item[ "date" ]
        title = item[ "title" ]
        summary_text = text_from_html(item[ "summary" ])
        link = item[ "link" ]
        json_object["timestamp"].append(dttm)
        json_object["title"].append(title)
        json_object["summary"].append(summary_text)
        json_object["link"].append(link)

# set the filename of json object using timestamp
timestr = time.strftime("%Y%m%d-%H%M%S")
filename = "reddit_feed_" + timestr

# run the function and pass the json_object
create_json(json_object, filename)

