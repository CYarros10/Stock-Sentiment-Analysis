from TwitterSearch import *
import pandas as pd

try:
    tso = TwitterSearchOrder() # create a TwitterSearchOrder object
    tso.set_keywords(['MSFT', '$MSFT', 'Microsoft Stock']) # let's define all words we would like to have a look for
    tso.set_language('en') # we want to see German tweets only
    tso.set_include_entities(False) # and don't give us all those entity information

    # it's about time to create a TwitterSearch object with our secret tokens
    ts = TwitterSearch(
            access_token = '355258725-ieNBIlHNqMM2KpdDZUbYDxDeGfNqQv6uclV5xb7l',
            access_token_secret = 'GQI4XFLLmcytfoOTV3BMQalyR583Z1gGDpwEE6nYQdehl',
            consumer_key = 'Tg2tQvfjYYB5wj5d1ib8mdQfU',
            consumer_secret = 'b47muF1CveaLrVzLFbDCSEUuHwRkQT9zuZww64TCnCwoaou3FT'
     )

    tweets = []

     # this is where the fun actually starts :)
    for tweet in ts.search_tweets_iterable(tso):
        tweetText = tweet['text']
        tweetRetweetCount = tweet['retweet_count']
        tweetCreatedAt = tweet['created_at']
        tweetUsername = tweet['user']['screen_name']
        tweetFollowersCount = tweet['user']['followers_count']
        tweets.append(
                    {
                    "username": tweetUsername,
                    "text": tweetText,
                    "created_at": tweetCreatedAt,
                    "retweet_count":tweetRetweetCount,
                    "followers_count": tweetFollowersCount
                     }
                    );


except TwitterSearchException as e: # take care of all those ugly errors if there are some
    print(e)

df = pd.DataFrame(pd.DataFrame.from_dict(tweets, orient='columns'))
fd = open('twitterSearch2Results.csv','a')
df.to_csv(fd, index=True, encoding='utf-8')