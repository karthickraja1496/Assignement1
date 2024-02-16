import googleapiclient.discovery
from googleapiclient.discovery import build
import googleapiclient.errors
import pandas as pd
import json
import streamlit as st
import mysql.connector
import re

def apiconnect():
    api_id="AIzaSyB7sXPwaCamDEPFNlv7ZbrYQigvacrmHTs"
    
    
    api_name="youtube"
    api_version="v3"
    
    youtube=build(api_name,api_version,developerKey=api_id)
    return youtube

youtube=apiconnect()

#channel details
def channel_details(channel_id):
    request=youtube.channels().list(
                part="contentDetails,snippet,statistics,localizations",
                id=channel_id
                )
    response = request.execute()
    
    
    for i in response['items']:
        details= dict(Channel_Name=i["snippet"]["title"],Channel_Description=i["snippet"]["description"],Channel_Id=i["id"],
                    Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],Subscription_Count=i["statistics"]["subscriberCount"],Channel_Views=i["statistics"]["viewCount"],
                    total_videos=i["statistics"]["videoCount"]
                    )
        return details

# playlistid
def getvideoid(channelid):
    video_id = []
    respect1 = youtube.channels().list(id=channelid, part="contentDetails").execute()

    playlist_id = respect1["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    nextpage = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',playlistId=playlist_id,maxResults=50,pageToken=nextpage).execute()

        for i in range(len(response1['items'])):
            video_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        nextpage=response1.get('nextPageToken')
        
        if nextpage is None:
            break
    return video_id

#video information
def videodetails(video):
    video_details=[]
    for video_id in video:
        request2=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response2=request2.execute()
        for detail in response2['items']:
            data=dict(Video_Id=detail['id'],
                    Channel_Name=detail["snippet"]["channelTitle"],
                    Channel_Id=detail["snippet"]["channelId"],
                    Video_Name=detail['snippet']['title'],
                    Video_Description=detail['snippet'].get('description'),
                    Tags=detail['snippet'].get('tags'),
                    PublishedAt=detail['snippet']['publishedAt'],
                    View_Count=detail['statistics'].get('viewCount'),
                    Like_Count=detail['statistics'].get('likeCount'),
                    Favorite_Count=detail['statistics']['favoriteCount'],
                    Comment_Count=detail['statistics'].get('commentCount'),
                    Duration=detail['contentDetails']['duration'],
                    Thumbnail=detail['snippet']['thumbnails']['default']['url'],
                    Caption_Status=detail['contentDetails']['caption']
                    )
            video_details.append(data)
    
    return video_details

#comment information

def getcomment(videoids):
    comment_info = []

    try:
        for video_id in videoids:
            # Request to get comments
            comments_request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )

            # Request to get video details
            video_request = youtube.videos().list(
                part="snippet",
                id=video_id
            )

            # Execute the comments request
            while comments_request:
                comments_response = comments_request.execute()

                for comment_details in comments_response.get('items', []):
                    comment_data = dict(
                        comment_id=comment_details['snippet']['topLevelComment']['id'],
                        video_id=comment_details['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=comment_details['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=comment_details['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published=comment_details['snippet']['topLevelComment']['snippet']['publishedAt'],
                        channel_id=comment_details['snippet']['channelId'],
                    )

                    comment_info.append(comment_data)

                comments_request = youtube.commentThreads().list_next(comments_request, comments_response)

            # Execute the video details request
            video_response = video_request.execute()
            video_details = video_response.get('items', [])[0]['snippet']

            # Add video details to each comment data
            for comment_data in comment_info:
                comment_data.update(
                Channel_Name=video_details["channelTitle"]
                )

    except Exception as e:
        print(f"An error occurred: {e}")

    return comment_info

#playlist details
def playlist_id(channelids):
    nextpage=None
    playlistdt=[]

    while True:
        request4=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channelids,
                maxResults=50,
                pageToken=nextpage    
        )
        response4=request4.execute()

        for details in response4['items']:
            data=dict(playlistid=details['id'],
                    title=details['snippet']['title'],
                    Channel_Id=details['snippet']['channelId'],
                    Channel_Name=details['snippet']['channelTitle'],
                    published_at=details['snippet']['publishedAt'],
                    video_count=details['contentDetails']['itemCount']
                    )
            playlistdt.append(data)
        
        nextpage=response4.get('nextPageToken')
        
        if nextpage is None:
            break
    return playlistdt

#mongodb

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://karthickraja1496:qwerty12345@cluster0.jdl8xl3.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

import pymongo
from pymongo.mongo_client import MongoClient


client = MongoClient("mongodb+srv://karthickraja1496:qwerty12345@cluster0.jdl8xl3.mongodb.net/?retryWrites=true&w=majority")

db=client['youtube_data']

#uploading data in mongodb
def channeldetails(channels_id):
    channel=channel_details(channels_id)
    play_id=playlist_id(channels_id)
    vid_id=getvideoid(channels_id)
    vid_details=videodetails(vid_id)
    com_ment=getcomment(vid_id)
    
    collect=db['ChannelDetails']
    collect.insert_one({'Channelinfo':channel,'Playlistinfo':play_id,'Videoinfo':vid_details,'Commentinfo':com_ment})
    
    return "upload successfully"

#connect to sql for table creation

import mysql.connector
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
    )
print(mydb)
mycursor=mydb.cursor(buffered=True)

def databases():
    drop_data="drop database if exists youtube_data"
    mycursor.execute(drop_data)
    mydb.commit()
    try:
        mycursor.execute("create database youtube_data")
        mycursor.execute("use youtube_data")
        
        createinfo='''create table channel_info(Channel_Name varchar(100),Channel_Id varchar(100) primary key,
                        Subscription_Count bigint,
                        Channel_Views bigint,
                        total_videos bigint,
                        Channel_Description text,
                        Playlist_Id varchar(100)
                        )'''
        mycursor.execute(createinfo)
        mydb.commit()
        
        createplay='''create table Playlist(playlistid varchar(100) primary key,title varchar(100),
                    Channel_Id varchar(100),
                    Channel_Name varchar(100),
                    published_at timestamp,
                    video_count int
                    )'''
        mycursor.execute(createplay)
        mydb.commit()
        
        createvideo='''create table Videoinfo(Video_Id varchar(20) primary key,
                                        Channel_Name varchar(300),
                                        Channel_Id varchar(100),
                                        Video_Name varchar(200),
                                        Video_Description text,
                                        Tags text,
                                        PublishedAt timestamp,
                                        View_Count bigint,
                                        Like_Count bigint,
                                        Favorite_Count int,
                                        Comment_Count int,
                                        Duration time,
                                        Thumbnail varchar(300),
                                        Caption_Status varchar(50)
                        )'''
        mycursor.execute(createvideo)
        mydb.commit()
        
        createcomment='''create table Commentinfo(comment_id varchar(200) primary key,
                                    video_id varchar(200),
                                    comment_text varchar(500),
                                    comment_author varchar(500),
                                    comment_published timestamp,
                                    channel_id varchar(100),
                                    Channel_Name varchar(100)
                                    )'''
                        
        mycursor.execute(createcomment)
        mydb.commit()
    except:
        print("database already created")
#mycursor.execute("create database youtubedata")


mydb.commit()

#channel table

def migrate_channel_data(mycursor, mydb, client,channel_name):
    channel_list = []
    db = client['youtube_data']
    collect = db['ChannelDetails']

    # Filter data based on the provided channel_id
    for channel_data in collect.find({"Channelinfo.Channel_Name": channel_name}):
        channel_list.append(channel_data['Channelinfo'])
    df = pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        query1= '''INSERT INTO channel_info(Channel_Name, Channel_Id, Subscription_Count, Channel_Views, total_videos, Channel_Description, Playlist_Id)
                VALUES(%s, %s, %s, %s, %s, %s, %s)'''
        value1 = (row['Channel_Name'], row['Channel_Id'], row['Subscription_Count'], row['Channel_Views'],
                row['total_videos'], row['Channel_Description'], row['Playlist_Id'])

        try:
            mycursor.execute(query1, value1)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

#playlist table

def migrate_playlist_data(mycursor, mydb, client, channel_name):
    playlist_list = []
    db = client['youtube_data']
    collect = db['ChannelDetails']

    # Filter data based on the provided channel_id
    for playlist_data in collect.find({"Playlistinfo.Channel_Name": channel_name}):
        for i in range(len(playlist_data['Playlistinfo'])):
            playlist_list.append(playlist_data['Playlistinfo'][i])
    df = pd.DataFrame(playlist_list)

    for index, row in df.iterrows():
        query = '''INSERT INTO Playlist(playlistid, title, Channel_Id, Channel_Name, published_at, video_count)
                VALUES(%s, %s, %s, %s, %s, %s)'''
        value = (row['playlistid'], row['title'], row['Channel_Id'], row['Channel_Name'], row['published_at'], row['video_count'])

        try:
            mycursor.execute(query, value)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

#video information table

def migrate_videoinfo_data(mycursor, mydb, client, channel_name):
    videoinfo_list = []
    db = client['youtube_data']
    collect = db['ChannelDetails']

    # Filter data based on the provided channel_id
    for videoinfo_data in collect.find({"Videoinfo.Channel_Name": channel_name}, {"_id": 0, "Videoinfo": 1}):
        for i in range(len(videoinfo_data['Videoinfo'])):
            videoinfo_list.append(videoinfo_data['Videoinfo'][i])
    df = pd.DataFrame(videoinfo_list)

    for index, row in df.iterrows():
        tag = json.dumps(row['Tags'])
        match = re.match(r'PT(\d+)M(\d+)S', row['Duration'])
        if match:
            minutes, seconds = map(int, match.groups())
        else:
            minutes, seconds = 0, 0
        duration_str = f'{minutes:02d}:{seconds:02d}'

        query = '''INSERT INTO Videoinfo(Video_Id, Channel_Name,Channel_Id, Video_Name, Video_Description, Tags, PublishedAt,
                                        View_Count, Like_Count, Favorite_Count, Comment_Count, Duration, Thumbnail,
                                        Caption_Status)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)'''
        value = (row['Video_Id'], row['Channel_Name'],row["Channel_Id"], row['Video_Name'], row['Video_Description'], tag,
                row['PublishedAt'], row['View_Count'], row['Like_Count'], row['Favorite_Count'], row['Comment_Count'],
                duration_str, row['Thumbnail'], row['Caption_Status'])

        try:
            mycursor.execute(query, value)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"MySQL Error: {err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


# comment table

def migrate_commentinfo_data(mycursor, mydb, client, channel_name):
    comment_info_list = []
    db = client['youtube_data']
    collect = db['ChannelDetails']

    # Filter data based on the provided channel_id
    for comment_data in collect.find({"Commentinfo.Channel_Name":channel_name}, {"_id": 0, "Commentinfo": 1}):
        for i in range(len(comment_data['Commentinfo'])):
            comment_info_list.append(comment_data['Commentinfo'][i])
    df = pd.DataFrame(comment_info_list)

    for index, row in df.iterrows():
        query = '''INSERT INTO Commentinfo(comment_id, video_id, comment_text, comment_author, comment_published,channel_id,Channel_Name)
                VALUES(%s, %s, %s, %s, %s,%s,%s)'''
        value = (row['comment_id'], row['video_id'], row['comment_text'], row['comment_author'], row['comment_published'],row['channel_id'],row['Channel_Name'])

        try:
            mycursor.execute(query, value)
            mydb.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

#function for data transfer from mongodb to sql
def migrate_data_to_mysql(channel_name):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient("mongodb+srv://karthickraja1496:qwerty12345@cluster0.jdl8xl3.mongodb.net/?retryWrites=true&w=majority")

        # Call the functions to migrate data for the specified channel_name
        migrate_channel_data(mycursor, mydb, client, channel_name)
        migrate_playlist_data(mycursor, mydb, client, channel_name)
        migrate_videoinfo_data(mycursor, mydb, client, channel_name)
        migrate_commentinfo_data(mycursor, mydb, client, channel_name)

        # Show tables
        mycursor.execute("SHOW TABLES")
        tables = mycursor.fetchall()
        print(tables)

    except pymongo.errors.ConnectionFailure as e:
        print(f"MongoDB Connection Error: {e}")
    except mysql.connector.Error as err:
        print(f"MySQL Connection Error: {err}")
    finally:
        # Close MongoDB connection
        client.close()
        # Close MySQL connection
        mydb.close()


def channelview():
    channel_list=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']
    mydb.commit()

    for channel_data in collect.find({},{"_id":0,"Channelinfo":1}):
        channel_list.append(channel_data['Channelinfo'])
    sd=st.dataframe(channel_list)
    
    return sd

# channel name from MongoDB
def get_channel_name_from_mongodb():
    client = pymongo.MongoClient("mongodb+srv://karthickraja1496:qwerty12345@cluster0.jdl8xl3.mongodb.net/?retryWrites=true&w=majority")
    db = client['youtube_data']
    collect = db['ChannelDetails']
    
    channel_nams = []

    for chlist in collect.find({}, {"_id": 0, "Channelinfo.Channel_Name": 1}):
        channel_nams.append(chlist['Channelinfo']['Channel_Name'])

    client.close()
    return channel_nams

#streamlit
import mysql.connector
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtube_data"
    
)
print(mydb)
mycursor=mydb.cursor(buffered=True)

st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

#get channel id

channel_id=st.text_input("Enter Channel ID")

col1,col2,col3=st.columns(3)

with col1:
    st.write(":orange[To Store Channel Details in MongoDB Click Below]")
    if st.button("Store Data in MongoDB"):
        channelids=[]
        db=client['youtube_data']
        collect1=db['ChannelDetails']
        mydb.commit()

        for chlist in collect1.find({},{"_id":0,"Channelinfo":1}):
            channelids.append(chlist['Channelinfo']["Channel_Id"])
            
        if channel_id in channelids:
            st.success("Channel Details of the given Channel ID already exists")
        else:
            insert=channeldetails(channel_id)
            st.success(insert)

with col2:
    st.write(":bold[To Create Database in SQL Click Below]")
    st.caption(":red[Do You Want to Create a New Database]")
    agree=st.checkbox("I Agree")
    if agree:
        if st.button("create database"):
            databases()
            st.warning("database successfully created")

# Function to migrate data to SQL
with col3:
    st.write(":green[Select the channel to migrate data from MongoDB to SQL]")
    channel_name_options = get_channel_name_from_mongodb()

    # Create a selectbox to choose a Channel ID
    selected_channel_name = st.selectbox("Select Channel Name:", channel_name_options, index=0)  # Set index to 0 to show the first option by default

        
    if st.button("Migrate data to MySQL") and selected_channel_name:
            # Trigger migration function only if a Channel ID is provided
        migrate_data_to_mysql(selected_channel_name)
        st.success("Data migration completed successfully!")

import mysql.connector
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtube_data'
)
print(mydb)
mycursor=mydb.cursor(buffered=True)

st.subheader(":violet[Details of the Available Channel]")
if st.button("Channel Details"):
    channelview()

Question=st.selectbox("Select Your Question",("1. What are the names of all the videos and their corresponding channels?",
                                            "2. Which channels have the most number of videos, and how many videos do they have?",
                                            "3. What are the top 10 most viewed videos and their respective channels?",
                                            "4. How many comments were made on each video, and what are their corresponding video names?",
                                            "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                            "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                            "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                            "8. What are the names of all the channels that have published videos in the year 2022?",
                                            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                            "10.Which videos have the highest number of comments, and what are their corresponding channel names?"

                                            
                                            ))

if Question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select Video_Name as title  ,Channel_Name as channelname from videoinfo'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    dv1=pd.DataFrame(t1,columns=['Video Title','Channel Name'])
    st.write(dv1)
    
elif Question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2='''select Channel_name as channelname, total_videos as no_of_videos from channel_info order by total_videos desc limit 1'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    dv2=pd.DataFrame(t2,columns=['Channel Name','Videos Count'])
    st.write(dv2)
    
elif Question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select View_Count as Views, Channel_name as channelname, Video_Name as videotitle from videoinfo 
                where View_Count is not null order by View_count desc limit 10 '''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    dv3=pd.DataFrame(t3,columns=['View Count','Channel Name','Video Title'])
    st.write(dv3)
    
elif Question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''select Comment_Count as No_of_Comments, Channel_name as channelname, Video_Name as videotitle from videoinfo 
                where Comment_Count is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    dv4=pd.DataFrame(t4,columns=['Comment Count','Channel Name','Video Title'])
    st.write(dv4)
    
elif Question== "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select Like_Count as No_of_Likes, Channel_name as channelname, Video_Name as videotitle from videoinfo 
                where Like_Count is not null order by Like_count desc limit 1'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    dv5=pd.DataFrame(t5,columns=['Likes','Channel Name','Video Title'])
    st.write(dv5)
    
elif Question== "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select Like_Count as No_of_Likes, Channel_Name as channelname, Video_Name as videotitle from videoinfo 
                where Like_Count is not null'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    dv6=pd.DataFrame(t6,columns=['Likes','Channel Name','Video Title'])
    st.write(dv6)
    st.warning("Dislikes-NO DATA AVAILABLE")

elif Question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select Channel_name as channelname, Channel_Views as no_of_views from channel_info'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    dv7=pd.DataFrame(t7,columns=['Channel Name','Total Views'])
    st.write(dv7)
    
elif Question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select Channel_Name as channelname,Video_Name as videotitle, PublishedAt as published from videoinfo
                where year(PublishedAt)=2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    dv8=pd.DataFrame(t8,columns=['Channel Name','Video Name','Published Date'])
    st.write(dv8)

elif Question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select Channel_Name as channelname,AVG (Duration) as averageduration from videoinfo
        group by Channel_Name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    dv9=pd.DataFrame(t9,columns=['Channel Name','Average Duration'])

    T9=[]
    for index,row in dv9.iterrows():
        channel_title=row["Channel Name"]
        avg_duration=row["Average Duration"]
        average_duration=str(avg_duration)
        T9.append(dict(Channelname=channel_title,Averageduration=average_duration))
    dvv=pd.DataFrame(T9)
    st.write(dvv)

elif Question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select Comment_Count as No_of_Comments, Channel_name as channelname, Video_Name as videotitle from videoinfo 
                    where Comment_Count is not null order by Comment_count desc limit 1'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    dv10=pd.DataFrame(t10,columns=['Comment Count','Channel Name', 'Video Name'])
    st.write(dv10)