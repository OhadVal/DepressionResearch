import Create_Data.UtilFunctions as utils
import pandas as pd

# Changed from a recursive function to an infinite loop.
# thus, no extra memory required.

es = utils.Elasticsearch("http://localhost:9200")

index_counter = es.count(index='test')
while True:

    reddit = utils.connectToAPI()
    new_subreddit = utils.getNewSubreddit(reddit, 2)
    submissionDF = utils.loadData()
    print("Current DataFrame Shape:{}".format(submissionDF.shape))

    unique_names = utils.getNames(submissionDF, new_subreddit)
    print("Number of new users:{}".format(len(unique_names)))

    if len(unique_names) == 0:
        print("Going to sleep")
        utils.time.sleep(60 * 20)
        print("Waking up")
        pass  # clear - works
    else:

        topics_dict = {
            "_submission_id": [],
            "_title": [],
            "_score": [],
            "_num_comments": [],
            "_title_length": [],
            "_subreddit": [],
            "_post_text": [],
            "_comment_karma": [],
            "_link_karma": [],
            "_upvote_ratio": [],
            "_date_created": [],
            "_user_name": [],
        }

        print("Entering Part 1\n")
        for curr_id in unique_names:
            print(curr_id)
            try:
                for submission in reddit.redditor(str(curr_id)).submissions.new():
                    userName = str(submission.author)
                    topics_dict['_submission_id'].append(submission.id)
                    topics_dict['_title'].append(submission.title)
                    topics_dict['_score'].append(submission.score)
                    topics_dict['_num_comments'].append(submission.num_comments)
                    topics_dict['_title_length'].append(len(submission.title))
                    topics_dict['_subreddit'].append(submission.subreddit)
                    topics_dict['_post_text'].append(submission.selftext)
                    topics_dict['_link_karma'].append(reddit.redditor(userName).link_karma)
                    topics_dict['_upvote_ratio'].append(submission.upvote_ratio)
                    topics_dict['_date_created'].append(submission.created_utc)
                    topics_dict['_user_name'].append(submission.author)
                    topics_dict['_comment_karma'].append(reddit.redditor(userName).comment_karma)
            except Exception as e:
                print("Error occured with id:{}".format(str(curr_id)))
                print(e)
                pass

        topics_dict = utils.pd.DataFrame(data=topics_dict)

        print("Entering Part 2")
        topics_dict = topics_dict[['_submission_id', '_title', '_score', '_num_comments',
                                   '_title_length', '_subreddit', '_post_text', '_comment_karma',
                                   '_link_karma', '_upvote_ratio', '_date_created', '_user_name']]
        topics_dict = utils.createMoreFeatures(topics_dict)

        print("Loading to Elasticsearch")
        topics_dict.to_csv('temp_json.csv')
        topics_dict = pd.read_csv('temp_json.csv')
        topics_dict.to_json('temp_json.json', orient='index')

        utils.init_elastic('test', 'test_doc', "http://localhost:9200", index_counter)

        # if index_counter == 0:
        #     utils.init_elastic('test', 'test_doc', "http://localhost:9200", i)
        # else:
        #     utils.init_elastic('test', 'test_doc', "http://localhost:9200", index_counter)

        index_counter = es.count(index='test')

        print("Saving")
        topics_dict = utils.pd.concat([topics_dict, submissionDF], sort=False)
        topics_dict = topics_dict.fillna('')

        topics_dict.to_csv('SubmissionsDF.csv')
