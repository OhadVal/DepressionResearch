'''

HIT - 2018/2019 -  Academic Research Project

Title - Analyzing depression symptoms in social media

@Author - Gilad Gecht, Ohad Valtzer, Alin Eliovich
'''

# ----------- Imports ----------- #
import time
import praw
import requests
import numpy as np
import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt

from time import time
from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer
from urllib3.exceptions import HTTPError
from datetime import datetime as dt


# ----------- Helper Functions ----------- #


# Load The Data
def loadData():
    submissionDF = pd.read_csv((r'C:\Users\Gilad\Desktop\SubmissionsDF.csv'))
    submissionDF = submissionDF.drop('Unnamed: 0',axis=1)
    return submissionDF

# Connect to reddit's API using Praw
def connectToAPI():

    reddit = praw.Reddit(client_id="v2iaLX1281gABQ",
                         client_secret="lYSXF__Z1ZlbUYqF_8F2ZwYOoNo",
                         user_agent="Gilad",
                         username="GiladGe",
                         password="gilad123")

    return reddit


# Connect to desired subreddit's new section
def getNewSubreddit(redditInstance, limit):
    subreddit = redditInstance.subreddit('AskReddit')
    new_subreddit = subreddit.new(limit=limit)

    return new_subreddit


# Iterate through the already asigned user names in the DataFrame,
# disregard existing user names and search for new not ones.
# If none found, process sleep for "X" not time.
# Otherwise, create a temporary DataFrame and concat to existing one.

def getNames(submissions, new_subreddit):
    list_of_names = list(set(submissions['_user_name'].tolist()))

    new_names = {"names": []}

    for submission in new_subreddit:  # works - clear
        if not submission.stickied:
            new_names["names"].append(submission.author)

    new_names = pd.DataFrame(data=new_names)
    new_names = new_names['names'].apply(lambda x: str(x))
    names_list = np.asarray(new_names)
    names_list = names_list.tolist()  # works - clear

    unique_new_names = []

    for nname in range(len(new_names)):
        curr_name = new_names[nname]
        if curr_name not in list_of_names:
            unique_new_names.append(curr_name)  # works - clear

    return unique_new_names


# Create new features and alter existing ones for better usage
def createMoreFeatures(submissionDF):
    submissionDF['_title_length'] = submissionDF['_title'].apply(lambda x: len(x))
    submissionDF['_num_words_title'] = submissionDF['_title'].apply(lambda x: len(x.split()))
    submissionDF['_post_length'] = submissionDF['_post_text'].apply(lambda x: len(x))
    submissionDF['_num_words_post'] = submissionDF['_post_text'].apply(lambda x: len(x.split()))
    submissionDF['_date_created'] = submissionDF['_date_created'].apply(lambda x: dt.fromtimestamp(x))
    submissionDF['_title'] = submissionDF['_title'].apply(lambda x: x.lower())
    submissionDF['_post_text'] = submissionDF['_post_text'].apply(lambda x: x.lower())

    return submissionDF


# Text Processing - Count Vectorizer and TfIdf Transformer
def vector_transformers(text_column):

    count_vect = CountVectorizer()
    post_text = count_vect.fit_transform(text_column)

    tfidf_transformer = TfidfTransformer()
    post_text_tfidf = tfidf_transformer.fit_transform(post_text)

    return post_text_tfidf


# Clean dataset
def clean_data(dataset):
    dataset = dataset[dataset['_subreddit'] != 'depression']
    dataset = dataset[dataset['_subreddit'] != 'AskReddit']
    dataset['_post_text'] = dataset['_post_text'].fillna('')
    dataset = dataset[dataset['_post_text'] != '[removed]']
    dataset = dataset.dropna()
    dataset = dataset.reset_index().drop('index', axis=1)

    return dataset