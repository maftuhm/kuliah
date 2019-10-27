
# coding: utf-8

# In[ ]:



from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


# variable that contains the user credentials to access Twitter API
access_token = "754099178-P6OHZBkvonuofUBO2i5eAWDx6uXIxKhmoB7xxVyF"
access_token_secret = "4ShZgKE4CXXaj6jmwz2er8jsao3RGRBIizYdVXU1kzIX0"
consumer_key = "AawFA1sh9dYWXIU18CV7FZyVk"
consumer_secret = "sKp3nIjwcUDAZNz8NSULxzy9HXFuIJ0aoRuSFhcFeI2p4I26nq"


# this is the basic listener that just prints received tweets to stdout
class StdOutListener(StreamListener):
    def on_data(self, data):
        #print ("%s\n" % data)
        #return True
        with open("twitter_tweets.txt", "a") as tweet_log:
            tweet_log.write(data)
            
    def on_error(self, status):
        print (status)


l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)


stream.filter(locations=[106.20, -6.41, 107.19, -6.01])



import json

# membaca data dalam format json
tweets_data = []
tweets_file = open('twitter_tweets.txt', "r")
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue



import pandas as pd

tweets = pd.DataFrame()
tweets['text'] = list(map(lambda tweet: tweet['text'], tweets_data))
tweets['lang'] = list(map(lambda tweet: tweet['lang'], tweets_data))
tweets['created_at'] = list(map(lambda tweet: tweet['created_at'], tweets_data))
tweets['country'] = list(map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, tweets_data))


print ("jumlah data tweets :", len(tweets))


# #### Langkah kelima : memilih data yang berbahasa indonesia


tweetsIn = tweets[tweets.lang == 'in']


tweetsIn.head()


print ("jumlah data tweetsIn :", len(tweetsIn))


# #### Langkah keenam : melakukan penyesuaian zona waktu
# 
# konversi tipe data pada kolom 'created_at' menjadi format 'datetime'

# In[14]:

tweetsIn['created_at']=pd.to_datetime(tweetsIn['created_at'], utc=True)


# merubah index tabel berdasarkan pada kolom 'created_at'

# In[15]:

tweetsIn.index = tweetsIn.created_at


# melihat hasil perubahan index

# In[16]:

tweetsIn.head()


# merubah zona waktu pada index 'created_at' yang merupakan standar UTC +0000 menjadi zona waktu 'Asia/Jakarta' +0700

# In[17]:

import pytz
from datetime import datetime
from pytz import timezone

JKT = pytz.timezone('Asia/Jakarta')
tweetsIn.index = tweetsIn.index.tz_localize(pytz.utc).tz_convert(JKT)


# melihat hasil perubahan zona waktu di kolom index

# In[18]:

tweetsIn.head()



tweetsIn25 = tweetsIn.loc['20170325']


tweetsIn25.head()


print ("jumlah data tweetsIn25 :", len(tweetsIn25))

df = pd.DataFrame({'tanggal' : ['25 Maret 2017'], 
                   'jumlah tweets' : [len(tweetsIn25)]})




# melihat grafik bar total tweets per hari

# In[28]:

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
x_pos = list(range(len(df)))
width = 0.8
plt.bar(x_pos, df['jumlah tweets'], width, alpha=1, color='r')
ax.set_ylabel('Jumlah Tweets', fontsize=12)
ax.set_title('Total Tweets per Hari', fontsize=12)
ax.set_xticks([p + 0.5 * width for p in x_pos])
ax.set_xticklabels(df['tanggal'])
plt.show()


# membuat tabel dan grafik data yang berisi jumlah total tweets per jam pada tanggal tertentu


import matplotlib.pyplot as plt
import seaborn as sns
from pylab import*
get_ipython().magic('matplotlib inline')
plt.rcParams['figure.figsize'] = (15, 5)


# membuat tabel dan grafik data jumlah tweets pada tanggal <b>25 Maret 2017</b>

from pandas import Series
def f(x):
     return Series(dict(Number_of_tweets = x['text'].count()))
    
hourly_count25 = tweetsIn25.groupby(tweetsIn25.index.hour).apply(f)
print ("data selama %d jam pada tanggal 25 Maret 2017" % len(hourly_count25))
hourly_count25


hourly_plot25 = hourly_count25['Number_of_tweets'].plot(kind='line')
hours = list(range(0,24))
xticks(np.arange(24), hours, rotation = 0,fontsize = 9)

hourly_plot25.set_title('Total Tweets per Jam Tanggal 25 Maret 2017', fontsize=15)
hourly_plot25.set_xlabel('Jam', weight='bold', labelpad=15)
hourly_plot25.set_ylabel('Jumlah Tweets', weight='bold', labelpad=15)

xticks(fontsize = 9, rotation = 0, ha= "center")
yticks(fontsize = 9)

hourly_plot25.tick_params(axis='x', pad=5)


hourly_plot25 = hourly_count25['Number_of_tweets'].plot(kind='bar')
hours = list(range(0,24))                                                 
xticks(np.arange(24), hours, rotation = 0,fontsize = 9)                   

hourly_plot25.set_title('Total Tweets per Jam Tanggal 25 Maret 2017', fontsize=15)
hourly_plot25.set_xlabel('Hour of the Day', weight='bold', labelpad=15)     
hourly_plot25.set_ylabel('# Tweets (Messages)', weight='bold', labelpad=15) 

xticks(fontsize = 9, rotation = 0, ha= "center")                          
yticks(fontsize = 9)                                                      
hourly_plot25.tick_params(axis='x', pad=5)                                  

