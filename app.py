from googleapiclient.discovery import build

import pymongo
import pymysql
import pandas as pd
import streamlit as st
from datetime import datetime


#Api key connection
def Api_connect():
    Api_Id = "AIzaSyCsbcSnkoxT7w_2csltLgJpFwQXdan1Zvc"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube


youtube = Api_connect()



# get channel information


def get_channel_info(channel_id):
	request = youtube.channels().list(
		  part = "snippet,ContentDetails,statistics",
		  id = channel_id
	)
	response = request.execute()
	for i in response ['items']:
			data = dict(Channel_Name = i["snippet"]["title"],
		    Channel_Id = i['id'],
		    Subscribers = i["statistics"]["subscriberCount"],
		    Views = i["statistics"]["viewCount"],
		    Total_Videos = i["statistics"]["videoCount"],
		    Channel_Description = i["snippet"]["description"],
		    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"]
              )
	return data


#get video ids

def get_video_ids(channel_id):

    video_ids = []
    response = youtube.channels().list(id = channel_id,
                                    part = "contentDetails").execute()
    Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:

        response1 = youtube.playlistItems().list(
                                                part = 'snippet',
                                                playlistId = Playlist_Id, 
                                                maxResults=50,
                                                pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids



#get video information

def get_video_info(video_ids):


    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()


        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = ",".join(item['snippet'].get('tags', ['NA'])),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount',0),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data



#get comment information

def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50)

            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id =item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_PublishedAt = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass

    return Comment_data


def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    collection1=db["channel_details"]
    collection1.insert_one({"channel_information":ch_details,
                            "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully!!"


#upload to mongoDB

client = pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]
collection1 = db["channel_details"]

# mysql connection
mydb = pymysql.connect(
    host = "localhost",
    user = "root",
    password = "1234",
    database = 'youtube_data'
)
cursor = mydb.cursor()

# channel table creation

def channels_table(channel_names):

    
    create_query="""CREATE TABLE IF NOT EXISTS channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key ,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))"""
    cursor.execute(create_query)
    mydb.commit()

   


    single_channel_detail=[]
    db = client ["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name":channel_names},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])

    df_single_channel_detail = pd.DataFrame(single_channel_detail)




    for index, row in df_single_channel_detail.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)

                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        

        try:
        
            cursor.execute(insert_query, values)
            mydb.commit()

        except:

            print("Channel values are already inserted!")


# Create table creation
def videos_table(channel_names):


    create_query = '''CREATE TABLE IF NOT EXISTS videos(Channel_Name VARCHAR(100),
                                                        Channel_Id VARCHAR(100),
                                                        Video_Id VARCHAR(50) PRIMARY KEY,
                                                        Title VARCHAR(150),
                                                        Thumbnail VARCHAR(200),
                                                        Description TEXT,
                                                        Published_Date TIMESTAMP,
                                                        Duration TIME,
                                                        Views BIGINT,
                                                        Likes BIGINT,
                                                        Comments BIGINT,
                                                        Favorite_Count INT,
                                                        Definition VARCHAR(100),
                                                        Caption_Status VARCHAR(100)
                                                        )'''
    
    
    cursor.execute(create_query)
    mydb.commit()

    single_video_detail=[]
    db = client ["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name":channel_names},{"_id":0}):
        single_video_detail.append(ch_data["video_information"])

    df_single_video_detail = pd.DataFrame(single_video_detail[0])


    df_single_video_detail['Duration'] = pd.to_timedelta(df_single_video_detail['Duration'])
    df_single_video_detail['Duration'] = df_single_video_detail['Duration'].astype(str)
    df_single_video_detail['Duration'] = df_single_video_detail['Duration'].str.extract(r'(\d+:\d+:\d+)')
    

    date_format = "%Y-%m-%dT%H:%M:%SZ"

    # Insert data into MySQL, ensuring only unique records are inserted
    for index, row in df_single_video_detail.iterrows():

        

        #tags_as_str = ' '.join([str(elem) for elem in row['Tags']])

        date_object = datetime.strptime(row['Published_Date'], date_format)
    

        insert_query = '''INSERT INTO videos(
                            Channel_Name,
                            Channel_Id,
                            Video_Id,
                            Title,
                            Thumbnail,
                            Description,
                            Published_Date,
                            Duration,
                            Views,
                            Likes,
                            Comments,
                            Favorite_Count,
                            Definition,
                            Caption_Status)
                            
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values = (row['Channel_Name'],  
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Thumbnail'],
                  row['Description'],
                  date_object,
                  row['Duration'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status'])
        
        try:
            cursor.execute(insert_query, values)
            mydb.commit() 

        except:
            print("Video values are already inserted!")

# creating comment table

def comments_table(channel_names):

    create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id  varchar(100) primary key,
                                                        Video_Id varchar (100),
                                                        Comment_Text text,
                                                        Comment_Author varchar(50),
                                                        Comment_PublishedAt timestamp)'''

    cursor.execute(create_query)
    mydb.commit()

    single_comment_detail=[]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_information.Channel_Name":channel_names},{"_id":0}):
        single_comment_detail.append(ch_data["comment_information"])

    df_single_comment = pd.DataFrame(single_comment_detail[0])

    date_format = "%Y-%m-%dT%H:%M:%SZ"

    # Insert data into MySQL, ensuring only unique records are inserted

    for index, row in df_single_comment.iterrows():

        date_object = datetime.strptime(row['Comment_PublishedAt'], date_format)

        insert_query = '''INSERT INTO comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_PublishedAt)

                            VALUES(%s,%s,%s,%s,%s)'''

        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  date_object)

        try:
            cursor.execute(insert_query, values)
            mydb.commit()

        except:
            print("Video values are already inserted!")


def tables(single_channel):
    channels_table(single_channel)
    videos_table(single_channel)
    comments_table(single_channel)

    return "Tables Created Successfully"


st.title("YouTube Data Harvesting and Warehousing")

# text box for channel id

channel_id = st.text_input("Enter youtube channel id below:")

with st.spinner():
    if st.button("Extract and upload to MongoDB"):
        ch_ids = []
        db = client['Youtube_data']
        colelction1 = db['channel_details']
        for ch_data in colelction1.find({}, {"_id": 0, "channel_information": 1}):
            ch_ids.append(ch_data['channel_information']['Channel_Id'])
        if channel_id in ch_ids:
            st.success("Channel details of the given channel id already exist")
        else:
            insert = channel_details(channel_id)
            st.success(insert)

# select box for channel names in mongodb

ch_names = []
db = client['Youtube_data']
collection1 = db['channel_details']
for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
    ch_names.append(ch_data['channel_information']['Channel_Name'])

    
channel_name = st.selectbox("Select Channel", ch_names)

# Button will be here

with st.spinner():

    if st.button("Submit"):
        creation_of_table = tables(channel_name)
        st.success(creation_of_table)

Question = st.selectbox("Select your Question", ("Choose a Question",
                                                  "1. What are the names of all the videos and their corresponding channels?",
                                                  "2. Which channels have the most number of videos, and how many videos do they have?",
                                                  "3. What are the top 10 most viewed videos and their respective channels?",
                                                  "4. How many comments were made on each video, and what are their corresponding video names?",
                                                  "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                  "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names",
                                                  "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                  "8. What are the names of all the channels that have published videos in the year 2022?",
                                                  "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                  "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))


if Question == "1. What are the names of all the videos and their corresponding channels?":

    Query1 = '''SELECT Title as videos, Channel_Name as channelname from videos'''
    cursor.execute(Query1)
    mydb.commit()
    t1 = cursor.fetchall()

    df1 = pd.DataFrame(t1, columns = ["Video Title", "Channel Name"])
    st.header(':red[Table]')
    st.write(df1)



elif Question == "2. Which channels have the most number of videos, and how many videos do they have?":

    Query2 = '''SELECT Channel_Name as channelname, Total_Videos as No_of_videos from channels order by total_videos desc'''
    cursor.execute(Query2)
    mydb.commit()
    t2 = cursor.fetchall()

    df2 = pd.DataFrame(t2, columns = ["Channel Name", "No of videos"])
    st.header(':red[Table]')
    st.write(df2)


elif Question == "3. What are the top 10 most viewed videos and their respective channels?":

    Query3 = '''SELECT Views as views, Channel_Name as channelname, title as videotitle from videos
                WHERE views is not null order by views desc limit 10'''
    cursor.execute(Query3)
    mydb.commit()
    t3 = cursor.fetchall()

    df3 = pd.DataFrame(t3, columns = ["Views", "Channel Name", "Video Title"])
    st.header(':red[Table]')
    st.write(df3)


elif Question == "4. How many comments were made on each video, and what are their corresponding video names?":

    Query4 = '''SELECT comments as no_of_comments, title as videotitle from videos where comments is not null'''
    cursor.execute(Query4)
    mydb.commit()
    t4 = cursor.fetchall()

    df4 = pd.DataFrame(t4, columns = ["No of comments", "Video Title"])
    st.header(':red[Table]')
    st.write(df4)


elif Question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":

    Query5 = '''SELECT title as videotitle, Channel_Name as channelname, likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(Query5)
    mydb.commit()
    t5 = cursor.fetchall()

    df5 = pd.DataFrame(t5, columns = ["Video Title", "Channel Name", "Like Count"])
    st.header(':red[Table]')
    st.write(df5)


elif Question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names":

    Query6 = '''SELECT likes as likecount, title as videotitle from videos'''
    cursor.execute(Query6)
    mydb.commit()
    t6 = cursor.fetchall()

    df6 = pd.DataFrame(t6, columns = ["Like Count", "Video Title"])
    st.header(':red[Table]')
    st.write(df6)


elif Question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":

    Query7 = '''SELECT Channel_Name as channelname, views as totalviews from channels'''
    cursor.execute(Query7)
    mydb.commit()
    t7 = cursor.fetchall()

    df7 = pd.DataFrame(t7, columns = ["Channel Name", "Total Views"])
    st.header(':red[Table]')
    st.write(df7)


elif Question == "8. What are the names of all the channels that have published videos in the year 2022?":

    Query8 = '''SELECT Channel_Name as channelname, Title as videotitle, Published_Date as videopublished from videos
                where extract(year from Published_Date)=2022 '''
    cursor.execute(Query8)
    mydb.commit()
    t8 = cursor.fetchall()

    df8 = pd.DataFrame(t8, columns = ["Channel Name", "Video Title", "Published Date"])
    st.header(':red[Table]')
    st.write(df8)


elif Question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    Query9 = '''SELECT Channel_Name as ChannelName, AVG(Duration) as AverageDuration from videos group by Channel_Name'''
    cursor.execute(Query9)
    mydb.commit()
    t9 = cursor.fetchall()

    df9 = pd.DataFrame(t9, columns = ["Channel Name", "Average Duration in Seconds"])
    st.header(':red[Table]')
    st.write(df9)


elif Question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":

    Query10 = '''SELECT Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos where comments is not null order by comments desc'''
    cursor.execute(Query10)
    mydb.commit()
    t10 = cursor.fetchall()

    df10 = pd.DataFrame(t10, columns = ["Video Title", "Channel Name", "Comments"])
    st.header(':red[Table]')
    st.write(df10)

