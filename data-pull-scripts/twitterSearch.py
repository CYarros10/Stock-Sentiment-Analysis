import twitter
import pandas as pd
import time
import re
from textblob import TextBlob
import cx_Oracle as co;


searchValue = 'ORCL'


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w +:\ / \ / \S +)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(clean_tweet(tweet))
    # set sentiment
    return analysis.sentiment.polarity


connection = co.connect('CIODashboardUser/password@129.158.70.193:1521/PDB1.gse00013232.oraclecloud.internal');
cursor = connection.cursor();

api = twitter.Api(
    consumer_key='i83JrbirFFJ9mx0xkgXacj7bT',
    consumer_secret='XbhsBbsECNBNOA0iDO9UqJRns5Xrjnx1gYrUGS5wFCh5Hm5vDz',
    access_token_key='940629705807597568-56AcrGEz8BVIVu8LbjOUARv4iuHf6m7',
    access_token_secret='aSWt2OP0MyNUud8I9tA58YEphTYK945yeh4Nx11qLgIsS')

tweets = []

# until parameter: Returns tweets created before the given date. Date should be formatted as YYYY-MM-DD. Keep in mind that the search
# index has a 7-day limit. In other words, no tweets will be found for a date older than one week.
search = api.GetSearch(raw_query="q=%24"+searchValue+"%20&result_type=mixed&count=100")

for tweet in search:
    tweetText = tweet.text
    tweetRetweetCount = tweet.retweet_count
    tweetCreatedAt = tweet.created_at
    tweetUsername = tweet.user.screen_name
    tweetFollowersCount = tweet.user.followers_count
    tweets.append(
        {
            "username": tweetUsername,
            "text": clean_tweet(tweetText).encode('utf-8'),
            "created_at": tweetCreatedAt,
            "retweet_count": tweetRetweetCount,
            "followers_count": tweetFollowersCount,
            "sentiment": get_tweet_sentiment(tweetText)
        }
    )

df = pd.DataFrame(pd.DataFrame.from_dict(tweets, orient='columns'))


#######################
# Clean up timestamp
#######################

timestamps = []
for i in df['created_at']:
    timestamps.append(time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(i,'%a %b %d %H:%M:%S +0000 %Y')))
df['timestamp'] = timestamps

times = []
dates = []
for i in df['timestamp']:
    dates.append(i.split()[0])
    times.append(i.split()[1])
df['entrydate'] = dates
df['entrytime'] = times
df['searchvalue'] = searchValue.encode('utf-8')

df.drop('created_at',axis=1, inplace=True)


df = df.sort_values(by=['entrydate','entrytime'])

###############################
# Insert results
###############################

print(df.head())
rows = [tuple(x) for x in df.values]
print(rows[0])
#cursor.executemany("insert /*+ ignore_row_on_dupkey_index(twitterdata, twitterdata_pk) */ into twitterdata (followers_count,retweet_count,sentiment,text,username,timestamp,entrydate,entrytime,searchvalue) values (:1, :2, :3, :4, :5, :6, :7, :8, :9)", rows)
#connection.commit()


fd = open('results/twitterSearchResults.csv','w')
df.to_csv(fd, index=True, encoding='utf-8', header=True)
