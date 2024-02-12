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
    commentinfo=[]
        
    try:
            for video_id in videoids:
                request3=youtube.commentThreads().list(
                    part="snippet", videoId=video_id,maxResults=50
                )
                    
                response3=request3.execute()
                    
                for details in response3['items']:
                    data=dict(commentid=details['snippet']['topLevelComment']['id'],
                            videoid=details['snippet']['topLevelComment']['snippet']['videoId'],
                            commenttext=details['snippet']['topLevelComment']['snippet']['textDisplay'],
                            commentauthor=details['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            commentpublished=details['snippet']['topLevelComment']['snippet']['publishedAt']
                            )
                    
                    commentinfo.append(data)
                        
    except:
        pass
        
    return commentinfo

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
                    channel_id=details['snippet']['channelId'],
                    channel_name=details['snippet']['channelTitle'],
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
    password="",
    database='youtubedata'
    
)
print(mydb)
mycursor=mydb.cursor(buffered=True)

#mycursor.execute("create database youtubedata")

mycursor.execute("use youtubedata")
mydb.commit()

#channel table

def channeltable():
    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtubedata'
    
    )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)
    drop_table="drop table if exists channel_info"
    mycursor.execute(drop_table)
    mydb.commit()

    try:
        createinfo='''create table channel_info(Channel_Name varchar(100),Channel_Id varchar(100) primary key,
                        Subscription_Count bigint,
                        Channel_Views bigint,
                        total_videos bigint,
                        Channel_Description text,
                        Playlist_Id varchar(100)
                        )'''
        mycursor.execute(createinfo)
        mydb.commit

    except:
        print("already created")


    #extract data from MongoDB

    channel_list=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']

    for channel_data in collect.find({},{"_id":0,"Channelinfo":1}):
        channel_list.append(channel_data['Channelinfo'])
    df=pd.DataFrame(channel_list)

    #creating table in sql and inserting data frame values from mongodb
    for index,row in df.iterrows():
        query='''insert into channel_info(Channel_Name,
                                        Channel_Id,
                                        Subscription_Count,
                                        Channel_Views,
                                        total_videos,
                                        Channel_Description,
                                        Playlist_Id)
                                        
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
        value=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            mycursor.execute(query,value)
            mydb.commit()
            
        except:
            print("details extracted already")

#playlist table

def playlisttable():
    mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtubedata'
    
    )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)
    drop_table="drop table if exists Playlist"
    mycursor.execute(drop_table)
    mydb.commit()


    createplay='''create table Playlist(playlistid varchar(100) primary key,title varchar(100),
                    channel_id varchar(100),
                    channel_name varchar(100),
                    published_at timestamp,
                    video_count int
                    )'''
    mycursor.execute(createplay)
    mydb.commit()

#extract mongodb
    play_list=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']

    for playst in collect.find({},{"_id":0,"Playlistinfo":1}):
        for i in range(len(playst['Playlistinfo'])):
            play_list.append((playst['Playlistinfo'][i]))
    df1=pd.DataFrame(play_list)
#creating table in sql and inserting data frame values from mongodb
    for index,row in df1.iterrows():
            query='''insert into Playlist(playlistid,
                                            title,
                                            channel_id,
                                            channel_name,
                                            published_at,
                                            video_count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
            value=(row['playlistid'],
                    row['title'],
                    row['channel_id'],
                    row['channel_name'],
                    row['published_at'],
                    row['video_count']
                    )
            
            try:
                mycursor.execute(query,value)
                mydb.commit()
                
            except:
                print("already exists")

#video information table

def videostable():
        drop_table="drop table if exists Videoinfo"
        mycursor.execute(drop_table)
        mydb.commit()


        createvideo='''create table if not exists Videoinfo(Video_Id varchar(20) primary key,
                                        Channel_Name varchar(300),
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

        video_info=[]
        db=client['youtube_data']
        collect=db['ChannelDetails']

        for vidlist in collect.find({},{"_id":0,"Videoinfo":1}):
                for i in range(len(vidlist['Videoinfo'])):
                        video_info.append((vidlist['Videoinfo'][i]))
        df2=pd.DataFrame(video_info)

        #creating table in sql and inserting data frame values from mongodb
        for index,row in df2.iterrows():
                tag = json.dumps(row['Tags'])
                
                match = re.match(r'PT(\d+)M(\d+)S', row['Duration'])
                if match:
                        minutes, seconds = map(int, match.groups())
                else:
                        minutes, seconds = 0, 0

                # Convert minutes and seconds to a time string in HH:MM:SS format
                duration_str = f'{minutes:02d}:{seconds:02d}'
                
                query='''insert into Videoinfo(Video_Id,
                                        Channel_Name,
                                        Video_Name,
                                        Video_Description,
                                        Tags,
                                        PublishedAt,
                                        View_Count,
                                        Like_Count,
                                        Favorite_Count,
                                        Comment_Count,
                                        Duration,
                                        Thumbnail,
                                        Caption_Status)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                value=(row['Video_Id'],
                        row['Channel_Name'],
                        row['Video_Name'],
                        row['Video_Description'],
                        tag,
                        row['PublishedAt'],
                        row['View_Count'],
                        row['Like_Count'],
                        row['Favorite_Count'],
                        row['Comment_Count'],
                        duration_str,
                        row['Thumbnail'],
                        row['Caption_Status']
                        )
                
                mycursor.execute(query,value)
                mydb.commit()


# comment table

def commenttable():
    mydb=mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtubedata'
    
    )
    print(mydb)
    mycursor=mydb.cursor(buffered=True)
    drop_table="drop table if exists Commentinfo"
    mycursor.execute(drop_table)
    mydb.commit()


    createcomment='''create table if not exists Commentinfo(commentid varchar(200) primary key,
                                    videoid varchar(200),
                                    commenttext varchar(500),
                                    commentauthor varchar(500),
                                    commentpublished timestamp
                    )'''
    mycursor.execute(createcomment)
    mydb.commit()

    comment_info=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']

    for comlist in collect.find({},{"_id":0,"Commentinfo":1}):
            for i in range(len(comlist['Commentinfo'])):
                    comment_info.append((comlist['Commentinfo'][i]))
    df3=pd.DataFrame(comment_info)

    #creating table in sql and inserting data frame values from mongodb
    for index,row in df3.iterrows():
            query='''insert into Commentinfo(commentid,
                                    videoid,
                                    commenttext,
                                    commentauthor,
                                    commentpublished
                                    )
                                            
                                            values(%s,%s,%s,%s,%s)'''
            value=(row['commentid'],
                    row['videoid'],
                    row['commenttext'],
                    row['commentauthor'],
                    row['commentpublished']
                    )
            
            mycursor.execute(query,value)
            mydb.commit()

import mysql.connector

def tables():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='youtubedata'
    )

    print(mydb)
    
    mycursor = mydb.cursor(buffered=True)
    
    channeltable()
    playlisttable()
    videostable()
    commenttable()
    mydb.commit()
        
    # Use mycursor instead of creating a new cursor
    mycursor.execute("SHOW TABLES")
    tabl = mycursor.fetchall()
    
    print(tabl)
    
    #mydb.commit()

    return "tables created"


def channelview():
    channel_list=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']
    mydb.commit()

    for channel_data in collect.find({},{"_id":0,"Channelinfo":1}):
        channel_list.append(channel_data['Channelinfo'])
    sd=st.dataframe(channel_list)
    
    return sd

def playlistview():
    play_list=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']
    mydb.commit()

    for playst in collect.find({},{"_id":0,"Playlistinfo":1}):
        for i in range(len(playst['Playlistinfo'])):
            play_list.append((playst['Playlistinfo'][i]))
    sd1=st.dataframe(play_list)
    
    return sd1

def videoview():
        video_info=[]
        db=client['youtube_data']
        collect=db['ChannelDetails']
        mydb.commit()

        for vidlist in collect.find({},{"_id":0,"Videoinfo":1}):
                for i in range(len(vidlist['Videoinfo'])):
                        video_info.append((vidlist['Videoinfo'][i]))
        sd2=st.dataframe(video_info)

        return sd2

def commentview():
    comment_info=[]
    db=client['youtube_data']
    collect=db['ChannelDetails']
    mydb.commit()

    for comlist in collect.find({},{"_id":0,"Commentinfo":1}):
            for i in range(len(comlist['Commentinfo'])):
                    comment_info.append((comlist['Commentinfo'][i]))
    sd3=st.dataframe(comment_info)
    
    return sd3

#streamlit
import mysql.connector
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="youtubedata"
    
)
print(mydb)
mycursor=mydb.cursor(buffered=True)
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    
channeldetails=st.text_input("Enter Channel ID")

if st.button("Collect and Store Data"):
    channelids=[]
    db=client['youtube_data']
    collect1=db['ChannelDetails']
    mydb.commit()

    for chlist in collect1.find({},{"_id":0,"Channelinfo":1}):
        channelids.append(chlist['Channelinfo']["Channel_Id"])
        
    if channeldetails in channelids:
        st.success("Channel Details of the given Channel ID already exists")
    else:
        insert=channel_details(channeldetails)
        st.success(insert)

if st.button("Migrate to SQL"):
    table=tables()
    st.success(table)
    mydb.commit()
    
    
tableview=st.radio("Select the Table for View",("Channels","Playlists","Videos","Comments"))

if tableview=="Channels":
    channelview()
    
elif tableview=="Playlists":
    playlistview()
    
elif tableview=="Videos":
    videoview()

elif tableview=="Comments":
    commentview()


import mysql.connector
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database='youtubedata'
)
print(mydb)
mycursor=mydb.cursor(buffered=True)

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
    st.header("Dislikes-NO DATA AVAILABLE")

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