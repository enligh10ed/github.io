# -*- coding: cp1252 -*-
# This script downloads and extracts information from slideshare.net 
# presentations. I developed and tested this code on:

# Python 2.7.11 --  64-bit
# Windows-10-10.0.10586

# I opted to use Python 2.7 because the slideshare package used in this 
# module is written in Python 2.7. As at the time I ran the code, 
# the program output was as follows:
# Number of presentations downloaded: 6743
# Program runtime: Approx. 244 minutes (needed to maintain slideshare.net crawl 
# policy of 5 second delay)

import re
import time
import urllib2
import hashlib
import requests
import threading
#!pip install slideshare #Uncomment this line to install slideshare if you don't already have it
import slideshare
import pandas as pd
import datetime as DT
from datetime import datetime
#!pip install bs4 #Uncomment this line to install bs4 if you don't already have it
from bs4 import BeautifulSoup

total_time = time.time()

# API credentials
api_key = 'your_key_here' # Provide your own valid key here
shared_secret = 'your_shared_secret_here' # Provide your own valid secret here

def download_slide(keyword):
    """Obtains slide IDs for slideshare.net presentations posted last week.
    Adapted from https://gist.github.com/gr-a-m/2c74dcbdbef8829a013b"""

    # Set up the auth for the request
    timestamp = int(time.time())
    m = hashlib.sha1()
    m.update(shared_secret.encode())
    m.update(str(timestamp).encode())

    # Set the parameters to pass in the request
    items_per_page = 50
    q = keyword
    detailed = 1

    # Send the request
    params = {
        "api_key": api_key,
        "ts": timestamp,
        "hash": m.hexdigest(),
        "items_per_page": items_per_page,
        "q": q,
        "sort": "mostviewed",
        "detailed": detailed,
        "lang": "en",
        "what": "tag"
    }
    r = requests.get("https://www.slideshare.net/api/2/search_slideshows", params=params)
    return(r.text)

# Download presentations from across different topics
topics = ["Art", "Automotive", "Business", "Career", "Data", "Analytics", "Design",
          "Devices", "Hardware", "Economy", "Finance", "Education", "Engineering",
          "Entertainment", "Humor", "Environment", "Food", "Government", "Nonprofit",
          "Health", "Medicine", "Healthcare", "Internet", "Investor Relations",
          "Law", "Leadership", "Management", "Lifestyle", "Marketing", "Mobile",
          "News", "Politics", "Presentations",  "Public Speaking", "Real Estate",
          "Recruiting", "Human Resources", "Retail", "Sales", "Science", 
          "Self Improvement", "Services", "Small Business", "Entrepreneurship",
          "Social-Media", "Software", "Spiritual", "Sports", "Technology", "Travel"]

slide_ids_db = []

# Extract all slide_ids contained in the slide
for topic in topics:
    result = download_slide(topic)
    list1 = re.findall('.*?\<ID\>(.*?)\</ID\>.*?', result)
    list2 = re.findall('.*?\>(.*?)\</RelatedSlideshowID\>.*?', result)
    slide_ids = list1 + list2
    slide_ids_db.append(slide_ids)

# Flatten list and remove duplicates
slide_ids_db = [item for sublist in slide_ids_db for item in sublist] 
slide_ids_db = list(set(slide_ids_db))

# In addition to slide id, extract the url and created date for each slide
# You will see a lot of "Insufficient permissions" message printed to the screen
# Apparently, some presentations have restricted access. 

urls = []
ids = []
dates_created = []
count = 0

flag = True
while flag and count < len(slide_ids_db):
    sl_id = slide_ids_db[count]
    api = slideshare.SlideshareAPI(api_key,shared_secret)
    try:
        slide = api.get_slideshow(slideshow_id=sl_id) 
    except (requests.HTTPError, urllib2.URLError, requests.ConnectionError) as e1:
        count += 1
        continue
    except slideshare.SlideShareServiceError as e2:
        if (e2.errmsg == 'Insufficient permissions'):
            count += 1
            continue
        if (e2.errmsg == 'Account Exceeded Daily Limit'):
            flag = False
            ##############################################################################
            # The regular api_key and shared_secret allow access to about 1002 slides only
            print("\nContact SlideShare.net if you want to increase your Daily Limit.\n") 
            ##############################################################################
            continue
    url = slide.values()[0]["URL"]
    url = url.encode('ascii','ignore')
    urls.append(url)
    date = slide.values()[0]["Created"]
    date = date.encode('ascii','ignore')
    dates_created.append(date)
    ids.append(sl_id)
    print(len(urls))
    count += 1


# Convert to data frame
slide_info_df = pd.DataFrame({'slide_id' : ids, 'url' : urls, 'date_created' : dates_created})

# From date_created, calculate the number of days the presentation is posted
# The format for date_created is 2014-10-01 or '2014-10-01 12:43:04 UTC'.

# First, convert date_created to datetime
f = lambda utc: datetime.strptime(str(utc), '%Y-%m-%d') if len(str(utc)) == 10  else datetime.strptime(str(utc)[:-4], '%Y-%m-%d %H:%M:%S')
slide_info_df.date_created = slide_info_df.date_created.apply(f)
    
# Second, convert datetime to date
slide_info_df.date_created = slide_info_df.date_created.apply(lambda dt: dt.date())

# Finally, calculate the number of days the presentation is posted
today = DT.date.today()
slide_info_df['days_posted'] = today - slide_info_df.date_created

# Crawl slideshare.net and get statistics for each presentation
# First, create variables to store the statistics
totalSlides = []
userLikes = []
userTweets = []
userComments = []
userPageVisits = []
images = []
ids2 = []
urls2 = []
dates_created = []
num_days_posted = []


crawl_delay = 5 # http://www.slideshare.net/robots.txt

start_time = time.time()

def progress():
    """The crawl takes a long time. This method will be used to print out progress report."""
    t = threading.currentThread()
    while getattr(t, "do_run", True):
        print("Time elapsed (mins): {0}\nNumber of urls scraped: {1}\n".format( \
        int((time.time()-start_time) /60), len(totalSlides)))
        time.sleep(120)
    print("\nStopped tracking / progress thread.\n")


t = threading.Thread(target=progress)
t.start()

# Now start crawling
for index, row in slide_info_df.iterrows():
    try:
        content = urllib2.urlopen(row.url)
        slide = content.read().decode("utf-8")
    except (urllib2.HTTPError, urllib2.URLError, UnicodeDecodeError, IOError) as e:
        print(e)
        continue
    
    try:
        # Get slideshow statistics
        result = re.search('"totalSlides":(.*?)(,|})', slide)
        numOfSlides = int(result.group(1))
        result = re.search('UserLikes:(.*?)"', slide)
        likes = int(result.group(1))
        result = re.search('UserTweets:(.*?)" ', slide)
        tweets = int(result.group(1))
        result = re.search('UserComments:(.*?)"', slide)
        comments = int(result.group(1))
        result = re.search('UserPageVisits:(.*?)"', slide)
        views = int(result.group(1))
        response = requests.get(row.url)
        soup = BeautifulSoup(response.content, 'html.parser')
        num_of_images = len(soup.find_all('img'))
        
        # Save statistics
        totalSlides.append(numOfSlides)
        userLikes.append(likes)
        userTweets.append(tweets)
        userComments.append(comments)
        userPageVisits.append(views)
        images.append(num_of_images)
        ids2.append(row.slide_id)
        urls2.append(row.url)
        dates_created.append(row.date_created)
        num_days_posted.append(row.days_posted)

    except (AttributeError, requests.ConnectionError, IOError) as e:
        print(e)
        continue
    time.sleep(crawl_delay)

# Stop progess thread
t.do_run = False
t.join()

# num_days_posted is of type Timedelta('682 days 00:00:00'). Convert to int. 
# Also replace 0 day with 1 day to avoid division by zero error when we do per day computations
num_days_posted2 = [int(str(timedelta).split(' d')[0]) for timedelta in num_days_posted]
num_days_posted3 = [1 if x == 0 else x for x in num_days_posted2]

# Persist data
presentations = pd.DataFrame({'slide_id' : ids2, 'total_slides' : totalSlides, 
                              'likes' : userLikes, 'tweets':userTweets, 
                              'comments' : userComments,'date_created' : dates_created,
                             'views' : userPageVisits, 'total_images' : images, 
                             'url' : urls2,  'days_posted' : num_days_posted3})
                             
presentations.to_csv("presentations.csv", index = False)

print("Program runtime: {0} mins".format(int((time.time() - total_time)/60.0)))
print("\nNumber of presentations downloaded: {0}\n".format(len(presentations)))