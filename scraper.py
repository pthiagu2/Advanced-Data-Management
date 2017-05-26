import socket, sys
from thread import *
import requests_oauthlib
import json
import oauth2 as oauth
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

HOST = ''  
PORT = int(sys.argv[1]) # Arbitrary port for the user to connect. 

# Variables that contains the user credentials to access Twitter API

consumer_key = "tEXlYXyYeDjWUpsBmJrl9LoNY"
consumer_secret = "VxUySH3f92c0LfGoGS3Vhqu3QAkknDAxJ8Ye8pHhtJyTzG0lre"
access_token = "2918853158-we3nJXB2inFQA3wdi9F0c0nEimmtJiDYuyTjnQW"
access_token_secret = "lcxt9HoC00fePoOGjdSXEUnDZ9psybSVnGgRCGHIAkVw8"
auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_token_secret)


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This listener sends the tweets on the connection when it receives the tweets.  
    """
    def __init__(self, connection):
        self.connection = connection

    def on_data(self, data):
        print(data+"\n")
        connection.send(data+"\n")
        return True

    def on_error(self, status):
        print(status)


#Function for handling connections. This will be used to read data from tweeter and write to socket

def clientthread(connection):
    # Stream using twitter API tweepy
    l = StdOutListener(connection)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(languages=["en"], locations=[-180,-90,180,90], track = ['travel', 'nature','seeing','enjoy','sports', 'game','tennis','ball','match'])

# Create a socket and bind it to the local host and port
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print 'Socket created'

try:
    s.bind((HOST, PORT))
except socket.error , msg:
    print 'Bind failed. Error Code : ' + str(msg[0]) + ' Reason ' + msg[1]
    sys.exit()

print 'Socket bind completed'

#Start listening on socket
s.listen(10)
print 'Socket listening'

while 1:
    # Accept a connection from the client user - blocking call
    connection, address = s.accept()
    print 'Connected with ' + address[0] + ':' + str(address[1])
    #start new thread takes first argument as a function name to be run, second is the tuple of arguments to the function.
    start_new_thread(clientthread ,(connection,))

s.close()