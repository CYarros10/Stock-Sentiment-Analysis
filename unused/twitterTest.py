from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


#consumer key, consumer secret, access token, access secret.
# Variables that contains the user credentials to access Twitter API
ACCESS_TOKEN = '355258725-ieNBIlHNqMM2KpdDZUbYDxDeGfNqQv6uclV5xb7l'
ACCESS_SECRET = 'GQI4XFLLmcytfoOTV3BMQalyR583Z1gGDpwEE6nYQdehl'
CONSUMER_KEY = 'Tg2tQvfjYYB5wj5d1ib8mdQfU'
CONSUMER_SECRET = 'b47muF1CveaLrVzLFbDCSEUuHwRkQT9zuZww64TCnCwoaou3FT'

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

class listener(StreamListener):

    def on_data(self, data):
        print(data)
        return(True)

    def on_error(self, status):
        print(status)

class StdOutListener(StreamListener):

    def on_data(self, data):
        #print data
        with open('fetched_tweets.txt','a') as tf:
            tf.write(data)
        return True

    def on_error(self, status):
        print(status)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["MSFT,$MSFT,#MSFT"],async=True)
