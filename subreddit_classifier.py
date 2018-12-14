'''
@Authors: Gilad Gecht, Ohad Valtzer, Alin Eliovich
Date Craeted: December 10th, 2018
'''

# ------- Imports ------- #
import pandas as pd
import Create_Data.UtilFunctions as utils
from sklearn.utils import shuffle
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.svm import LinearSVC


# ------- Load Data ------- #
df = pd.read_csv('https://raw.githubusercontent.com/GiladGecht/DepressionResearch/master/depression_neutral_df.csv')

# ------- Data Preprocessing ------- #

'''
Encoding the target label => Subreddit Name to 0/1, Neutral or Depression
For the current moment, we're keeping empty posts, this might be a key feature,
perhaps depressed people tend to write way more than a non-depressed person.
Since removed doesn't imply anything, we'll remove rows which contain such behaviour.
Eventually, we'll remove any other signs of Nulls in the dataframe.
'''

df = shuffle(df)
encoder = LabelEncoder()
df['_subreddit'] = encoder.fit_transform(df['_subreddit'])
df['_post_text'] = df['_post_text'].fillna('')
df = df[df['_post_text'] != '[removed]']
df = df.dropna()

# ------- Splitting Data to Features & Label ------- #

# TODO: For the time being, we created a numerical based classifier
#       Need to use text preprocessing (CountVectorizer, TfIDF...)
#       to improve classification


numeric_cols = ['_score', '_num_comments', '_title_length', '_comment_karma', '_link_karma',
        '_upvote_ratio', '_num_words_title',
        '_post_length', '_num_words_post']

target = '_subreddit'
cols = '_post_text'

X = df[cols]
y = df[target]
X = utils.vector_transformers(X)

# ------- Splitting to Train & Test ------- #
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ------- Model Training ------- #

# TODO: For the time being, we're using a logistic regression model,
#       might need to consider Naive Bayes classifiers in the future

svc = LinearSVC()
svc.fit(X_train, y_train)
y_pred = svc.predict(X_test)
score = svc.score(X_test, y_test)

# ------- Model Evaluation ------- #
print("score:",score)
print(confusion_matrix(y_pred=y_pred,y_true=y_test))


# ------- Predict Real Data ------- #
whole_data = pd.read_csv(r'C:\Users\Gilad\Desktop\SubmissionsDF.csv',index_col=0)
whole_data = utils.clean_data(whole_data)
whole_data['_predicted'] = svc.predict(whole_data[cols])

# ------- Check Results ------- #
print("\n")
for i in range(len(whole_data)):
        print((whole_data[['_subreddit','predicted']].iloc[i,:]))
        print("---------")