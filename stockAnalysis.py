import twitter
import pandas as pd
import time
import re
from textblob import TextBlob
import cx_Oracle as co
from pytrends.request import TrendReq
import threading
from datetime import datetime
from dateutil import tz
from alpha_vantage.techindicators import TechIndicators


#######################################################
# Clean up a tweet using regex
#######################################################
def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w +:\ / \ / \S +)", " ", tweet).split())


#######################################################
# Score a tweet based on the word sentiment
#######################################################
def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(clean_tweet(tweet))
    # set sentiment
    return analysis.sentiment.polarity


##########################################################
# Convert UTC times to EST to preserve consistency across
# data sources
##########################################################
def utcToEST(df):

    easternTimeStamps = []
    for timestamp in df['timestamp']:
        from_zone = tz.tzutc()
        to_zone = tz.tzlocal()

        utc = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

        # Tell the datetime object that it's in UTC time zone since
        # datetime objects are 'naive' by default
        utc = utc.replace(tzinfo=from_zone)

        # Convert time zone
        est = utc.astimezone(to_zone)
        est = datetime.strftime(est, '%Y-%m-%d %H:%M:%S')

        easternTimeStamps.append(str(est))

    df['timestamp'] = easternTimeStamps
    return df


##########################################################
# Clean up dataframe
#   - add data, time cols
#   - sort by datetime
##########################################################
def cleanDateTimes(df):
    times = []
    dates = []
    for i in df['timestamp']:
        dates.append(i.split()[0])
        times.append(i.split()[1])
    df['entrydate'] = dates
    df['entrytime'] = times
    df = df.sort_values(by=['entrydate', 'entrytime'])
    return df


##########################################################
# Open an existing file and overwrite it. If it does not
# exist, create it.
##########################################################
def saveCSV(df, fileName):
    fd = open(fileName, 'w')
    df.to_csv(fd, index=True, encoding='utf-8', header=True)
    fd.close()


##########################################################
# Create an Oracle Database connection and insert the
# dataframe contents via the desired query
##########################################################
def insertToOracleDatabase(df, query):
    connection = co.connect('CY/WElcome_123#@129.146.87.82:1521/pdb1.sub10231952570.newvcn.oraclevcn.com')
    cursor = connection.cursor()

    rows = [tuple(x) for x in df.values]
    cursor.executemany(query, rows)
    connection.commit()


#######################################################
# Pull stock data from alpha vantage
#######################################################
def stockPricePull(stockticker):
    print("Accessing stock price data for "+stockticker+" ...")

    df = pd.read_csv(
        'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + stockticker + '&interval=1min&apikey=1HAYLUHGCB6E0VXC&datatype=csv')

    df = cleanDateTimes(df)
    df['stockticker'] = stockticker

    #print(df.head())
    #saveCSV(df, 'results/stockTickerResults.csv')

    # Insert results into DB
    insertToOracleDatabase(df, '''insert /*+ ignore_row_on_dupkey_index(stockprice, stockprice_pk) */ 
                                into stockprice (timestamp,open,high,low,close,volume,entrydate, entrytime,stockticker)
                                values (:1, :2, :3, :4, :5, :6, :7, :8, :9)''')

    saveCSV(df, 'results/rawStockPriceData.csv')

#######################################################
# Pull data from google API
# params: phrase - pull data relating to this value
#######################################################
def googleTrendPull(stockticker):

    print("Retrieving google trends for "+stockticker+" ...")

    # Login to Google. Only need to run this once, the rest of requests will use the same session.
    pytrend = TrendReq()

    # Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()
    pytrend.build_payload(kw_list=[stockticker], timeframe='now 1-H')

    # Interest Over Time
    interest_over_time_df = pytrend.interest_over_time()

    df = pd.DataFrame(pd.DataFrame.from_dict(interest_over_time_df, orient='columns'))

    saveCSV(df, 'results/rawGoogleTrendData.csv')

    df = pd.read_csv('results/rawGoogleTrendData.csv')

    df.rename(columns={'date': 'timestamp'}, inplace=True)


    df = utcToEST(df)
    df = cleanDateTimes(df)

    df.drop('isPartial', axis=1, inplace=True)

    df["stockticker"] = stockticker

    saveCSV(df, 'results/rawGoogleTrendData.csv')

    # Insert results
    insertToOracleDatabase(df, '''insert /*+ ignore_row_on_dupkey_index(googletrend, googletrend_pk) */
                               into googletrend(timestamp,trendscore,
                              entrydate,entrytime,stockticker) values (:1, :2, :3, :4, :5)''')


#######################################################
# Pull data from twitter search API
# params: phrase - pull data relating to this value
#######################################################
def twitterSentimentPull(stockticker):

    print("Analyzing twitter sentiment for "+stockticker+" ...")


    api = twitter.Api(
        consumer_key='i83JrbirFFJ9mx0xkgXacj7bT',
        consumer_secret='XbhsBbsECNBNOA0iDO9UqJRns5Xrjnx1gYrUGS5wFCh5Hm5vDz',
        access_token_key='940629705807597568-56AcrGEz8BVIVu8LbjOUARv4iuHf6m7',
        access_token_secret='aSWt2OP0MyNUud8I9tA58YEphTYK945yeh4Nx11qLgIsS')

    tweets = []

    # until parameter: Returns tweets created before the given date. Date should be formatted as YYYY-MM-DD. Keep in mind that the search
    # index has a 7-day limit. In other words, no tweets will be found for a date older than one week.
    search = api.GetSearch(raw_query="q=%24" + stockticker + "%20&result_type=mixed&count=100")


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
        timestamps.append(time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(i, '%a %b %d %H:%M:%S +0000 %Y')))
    df['timestamp'] = timestamps

    df = utcToEST(df)
    df = cleanDateTimes(df)
    df['searchvalue'] = stockticker.encode('utf-8')

    df.drop('created_at', axis=1, inplace=True)

    # Insert results
    insertToOracleDatabase(df, '''insert /*+ ignore_row_on_dupkey_index(twittersentiment, twittersentiment_pk) */
                                into twittersentiment (followers_count,retweet_count,sentiment,text,username,
                                timestamp,entrydate,entrytime,searchvalue) 
                                values (:1, :2, :3, :4, :5, :6, :7, :8, :9)''')

    saveCSV(df, 'results/rawTwitterSentimentData.csv')


##########################################################
# Pull data from every source at once
##########################################################
def dataPull(stockTicker):

    print("Gathering prices, trends, and sentiment data for: "+stockTicker)

    stockPricePull(stockTicker)
    print("done!")
    twitterSentimentPull(stockTicker)
    print("done!")
    googleTrendPull(stockTicker)

    print("done!")


##########################################################
# Begin process
##########################################################
def begin():

    print("---- New Data Pull ----")
    print("Start: "+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    threading.Timer(3600.0, begin).start()

    dataPull("ORCL")
    dataPull("MSFT")
    dataPull("AMZN")
    dataPull("IBM")

    print("---- Waiting ----")

begin()