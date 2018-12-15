'''
@Authors: Gilad Gecht, Ohad Valtzer, Alin Eliovich
Date Craeted: December 10th, 2018
'''

# ------- Imports ------- #
import pandas as pd
import numpy as np
import Create_Data.UtilFunctions as utils

from sklearn.utils import shuffle
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.model_selection import train_test_split,cross_val_score
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

target = '_subreddit'
cols = '_post_text'

X = df[cols]
y = df[target]

count_vect = CountVectorizer(stop_words='english', lowercase=True,analyzer='word')
X = count_vect.fit_transform(X)
tfidf_transformer = TfidfTransformer()
X = tfidf_transformer.fit_transform(X)



# ------- Splitting to Train & Test ------- #

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ------- Model Training ------- #

svc = LinearSVC(random_state=42, penalty='l2', dual= True, tol=0.0001, C = 1,
                fit_intercept= True, intercept_scaling=1.0, class_weight= None)
svc.fit(X_train, y_train)
y_pred = svc.predict(X_test)
score = svc.score(X_test, y_test)

# ------- Model Evaluation ------- #
print("Accuracy Score:",score)
print(confusion_matrix(y_pred=y_pred,y_true=y_test))
print("AUC Score:", np.mean(cross_val_score(svc, X_train, y_train, cv=5, scoring='roc_auc')))

# ------- Predict Real Data ------- #
whole_data = pd.read_csv(r'/Users/giladgecht/Desktop/SubmissionsDF.csv',index_col=0)
whole_data = utils.clean_data(whole_data)
whole_data['_predicted'] = svc.predict(count_vect.transform(whole_data[cols]))

# ------- Check Results ------- #
print("\n")
for i in range(len(whole_data)):
        print((whole_data[['_subreddit','_predicted']].iloc[i,:]))
        print("---------")