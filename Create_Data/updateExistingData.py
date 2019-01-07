import Create_Data.UtilFunctions as utils
import pandas as pd

# Changed from a recursive function to an infinite loop.
# thus, no extra memory required.

index = 'reddit'
doc_type = 'submission'
es = utils.Elasticsearch("http://localhost:9200")
if es.indices.exists(index=index):
    index_counter = es.count(index=index)
else:
    es.indices.create(index=index, ignore=400)
    index_counter = es.count(index=index)


reddit = utils.connectToAPI()
submissionDF = utils.loadData()
print("Current DataFrame Shape:{}".format(submissionDF.shape))

unique_names = submissionDF['user_name'].unique()
print("Number of unique users:{}".format(len(unique_names)))


topics_dict = {
    "submission_id": [],
    "title": [],
    "score": [],
    "num_comments": [],
    "title_length": [],
    "subreddit": [],
    "post_text": [],
    "comment_karma": [],
    "link_karma": [],
    "upvote_ratio": [],
    "date_created": [],
    "user_name": [],
    "appearance": [],
}

print("Entering Part 1\n")
users_list = []
for curr_id in unique_names:
    new_submissions = 0
    print(curr_id)
    users_list.append(curr_id)
    print("Number of posts: ", submissionDF['user_name'].value_counts()[curr_id])
    # Insert of all of a user's posts from Reddit into a dictionary
    try:
        for submission in reddit.redditor(str(curr_id)).submissions.new():
            userName = str(submission.author)
            topics_dict['submission_id'].append(submission.id)
            topics_dict['title'].append(submission.title)
            topics_dict['score'].append(submission.score)
            topics_dict['num_comments'].append(submission.num_comments)
            topics_dict['title_length'].append(len(submission.title))
            topics_dict['subreddit'].append(submission.subreddit)
            topics_dict['post_text'].append(submission.selftext)
            topics_dict['link_karma'].append(reddit.redditor(userName).link_karma)
            topics_dict['upvote_ratio'].append(submission.upvote_ratio)
            topics_dict['date_created'].append(submission.created_utc)
            topics_dict['user_name'].append(submission.author)
            topics_dict['comment_karma'].append(reddit.redditor(userName).comment_karma)
            topics_dict['appearance'].append(-1)

    except Exception as e:
        print("Error occured with id:{}".format(str(curr_id)))
        print(e)
        pass


topics_dict = utils.update_data(topics_dict, submissionDF[submissionDF['user_name'].isin(users_list)])
topics_dict = utils.pd.DataFrame(data=topics_dict)

if len(topics_dict) > 0:
    print("Entering Part 2")
    topics_dict = topics_dict[['submission_id', 'title', 'score', 'num_comments',
                               'title_length', 'subreddit', 'post_text', 'comment_karma',
                               'link_karma', 'upvote_ratio', 'date_created', 'user_name', 'appearance']]
    topics_dict = utils.createMoreFeatures(topics_dict)

    print("Loading to Elasticsearch")
    print(len(topics_dict))
    topics_dict.to_csv('temp_json.csv',index=False)
    topics_dict = pd.read_csv('temp_json.csv')
    topics_dict.to_json('temp_json.json', orient='index')

    utils.init_elastic(index=index, doc_type=doc_type, elastic_address="http://localhost:9200", index_counter=index_counter)
    index_counter = es.count(index=index)
    print("Saving")
    topics_dict = utils.pd.concat([topics_dict, submissionDF], sort=False)
    topics_dict = topics_dict.fillna('')

    topics_dict.to_csv('SubmissionsDF2.csv', index=False)
