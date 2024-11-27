## Installation Note ####

'''
 Note that you need to install the tweepy and kafka-python libraries to run this code. 
 You can install these libraries using the following command (from the command line)

pip install tweepy 
pip install kafka-python
'''

import tweepy
import re
import csv
import secrets
from textblob import TextBlob


from kafka.producer import KafkaProducer

# Twitter API authentication
bearer_token = secrets.bearer_token

# Create an OAuth2 user handler using the bearer token
auth = tweepy.OAuth2BearerHandler(bearer_token)

#auth.set_access_token(secrets.Access_Token, secrets.Access_Token_Secret)

# Create the Twitter API client
api = tweepy.API(auth)

# Kafka producer configuration
topic = "twitter-sentiments"
brokers = "localhost:9092"

# Create the Kafka producer
producer = KafkaProducer(bootstrap_servers=brokers)

# Define the search query
query = ["toronto", "covid", "technology"]

#Clean text
def cleanText(text):
  text = text.lower()
  # Removes all mentions (@username) from the tweet since it is of no use to us
  text = re.sub(r'(@[A-Za-z0-9_]+)', '', text)
    
  # Removes any link in the text
  text = re.sub('http://\S+|https://\S+', '', text)

  # Only considers the part of the string with char between a to z or digits and whitespace characters
  # Basically removes punctuation
  text = re.sub(r'[^\w\s]', '', text)

  return text

def getsentiment(cleaned_text):
  # Returns the sentiment based on the polarity of the input TextBlob object
  if cleaned_text.sentiment.polarity > 0:
    return 'positive'
  elif cleaned_text.sentiment.polarity < 0:
    return 'negative'
  else:
    return 'neutral'

csvfile = open('Tweets.csv', 'a')
csvwriter = csv.writer(csvfile)
csvwriter.writerow(["id", "text", "likes", "retweet_count", "cleaned_text", "sentiment"])

for keyword in query:
    # Get the tweets from Twitter
    for tweet in tweepy.Cursor(api.search_tweets, q=keyword, tweet_mode = 'extended', count = 100).items():
        # Iterate over the tweets and send them to the Kafka topic
        # Note that these tweets are not being filtered in any way so output may not be very nice!
        clean_text = cleanText(tweet.full_text.encode('utf-8').decode('utf-8'))
        sentiment = getsentiment(TextBlob(clean_text))
        csvwriter.writerow([tweet.id_str, str(tweet.full_text.encode('utf-8')), tweet.favorite_count, tweet.retweet_count, str(clean_text), sentiment])
        # Send the JSON string to the Kafka topic
        producer.send(topic, tweet.full_text.encode('utf-8'))
        producer.flush()
    csvfile.close()
