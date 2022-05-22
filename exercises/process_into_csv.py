#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import pandas as pd
from google.cloud import storage, language_v1
import getpass
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import time
import datetime

# Define values of project, bucket_name, and url
PROJECT='umc-dsa-8420-fs2021'
bucket_name = 'bmgwd9-final-bucket'
storage_client = storage.Client(project=PROJECT)
bucket = storage_client.get_bucket(bucket_name)


    
def sample_analyze_sentiment(text_content):
    """
    Analyzing Sentiment in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en" # Must use or will get errors regarding unsupported languages
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_sentiment(request = {'document': document, 'encoding_type': encoding_type})
    
    output = {'sentiment_score': response.document_sentiment.score, 'sentiment_magnitude': response.document_sentiment.magnitude}
    return output

master_df = pd.DataFrame()
num_processed = 0
for blob in bucket.list_blobs():
    post_dict = json.loads(blob.download_as_string(client=None))
    # Initialize a new dictionary to contain previous information and sentiment scores
    my_dict = {"title":[], "summary":[], "timestamp": [], "link":[], "title_sentiment_score":[],
                     "title_sentiment_magnitude":[], "summary_sentiment_score":[], "summary_sentiment_magnitude":[]}
    
    for i in range(len(post_dict['title'])):
        incompatible_timestamp = post_dict['timestamp'][i]
        compatible_timestamp = incompatible_timestamp.replace("T", " ")
        my_dict["timestamp"].append(compatible_timestamp)
        my_dict["title"].append(post_dict['title'][i])
        my_dict["summary"].append(post_dict['summary'][i])
        my_dict["link"].append(post_dict['link'][i])
        
        title = post_dict['title'][i] # get title[i]
        title_nl_output = sample_analyze_sentiment(title)
        print(title_nl_output)
        my_dict["title_sentiment_score"].append(title_nl_output["sentiment_score"])
        my_dict["title_sentiment_magnitude"].append(title_nl_output["sentiment_magnitude"])
        
        summary = post_dict['summary'][i]
        summary_nl_output = sample_analyze_sentiment(summary)
        print(summary_nl_output)
        my_dict["summary_sentiment_score"].append(summary_nl_output["sentiment_score"])
        my_dict["summary_sentiment_magnitude"].append(summary_nl_output["sentiment_magnitude"])
        
        
    # convert processed dictionary to dataframe
    df = pd.DataFrame.from_dict(my_dict)
    
    master_df = pd.concat([master_df, df])
    
    num_processed += 1 # Counts the number of json objects processed

    # Limit the number of api calls to save money
    if num_processed >= 200:
        break    
    
    # Wait 10 seconds after procesing each json to avoid api call cap
    time.sleep(10)

print(master_df.shape)
master_df.to_csv('test_output.csv', index=False)


# #### When you are ready, let's export this
#  1. Open terminal
#  2. Change into course, module6, practices folder
#  3. Convert to a script: `jupyter-nbconvert --to script StorageTestCode.ipynb`
# ```
# [NbConvertApp] Converting notebook StorageTestCode.ipynb to script
# [NbConvertApp] Writing 4740 bytes to StorageTestCode.py
# ```
# 
# #### Move the generated script using `scp` or copy and paste techniques to get it to your VM.
