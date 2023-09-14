import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import isodate
from streamlit_option_menu import option_menu



st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing | By Mariya Stephen",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Mariya Stephen!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                       icons=["house-door-fill","tools","card-text"],
                       default_index=0,
                       orientation="vertical",
                       styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                            "--hover-color": "#C80101"},
                               "icon": {"font-size": "30px"},
                               "container" : {"max-width": "6000px"},
                               "nav-link-selected": {"background-color": "#C80101"}})





api_key = "AIzaSyAfZ20GixY3APY-VYL1o5vQlZGbjObJAU4" #"AIzaSyAfZ20GixY3APY-VYL1o5vQlZGbjObJAU4"

youtube = build('youtube','v3',developerKey=api_key)
api_service_name="youtube"
api_version="v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

mb = psycopg2.connect(host="localhost",user="postgres",password="stephen",database= "youtube",port = "5432")
cursor=mb.cursor()


def format_duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60
    formatted_duration = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return formatted_duration


def get_channel_sts(youtube,channel_id):
  
  request=youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id
  )
  response=request.execute()

  for item in response['items']: 
    data={'channelName':item['snippet']['title'],
          'channelId':item['id'],
          'subscribers':item['statistics']['subscriberCount'],
          'views':item['statistics']['viewCount'],
          'totalVideos':item['statistics']['videoCount'],
          'playlistId':item['contentDetails']['relatedPlaylists']['uploads'],
          'channel_description':item['snippet']['description']
    }    
  return data



def get_playlists(youtube,channel_id):
  request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25
    )
  response = request.execute()
  All_data=[]
  for item in response['items']: 
     data={'PlaylistId':item['id'],
           'Title':item['snippet']['title'],
           'ChannelId':item['snippet']['channelId'],
           'ChannelName':item['snippet']['channelTitle'],
           'PublishedAt':item['snippet']['publishedAt'],
           'VideoCount':item['contentDetails']['itemCount']
           }
     All_data.append(data)

     next_page_token = response.get('nextPageToken')
    
     while next_page_token is not None:

          request = youtube.playlists().list(
              part="snippet,contentDetails",
              channelId=channel_id,
              maxResults=25)
          response = request.execute()

          for item in response['items']: 
                data={'PlaylistId':item['id'],
                      'Title':item['snippet']['title'],
                      'ChannelId':item['snippet']['channelId'],
                      'ChannelName':item['snippet']['channelTitle'],
                      'PublishedAt':item['snippet']['publishedAt'],
                      'VideoCount':item['contentDetails']['itemCount']}
                All_data.append(data)
          next_page_token = response.get('nextPageToken')
  return All_data


def get_video_ids(youtube, playlist_id):
  request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
  response = request.execute()

  video_ids = []

  for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

  next_page_token = response.get('nextPageToken')
  more_pages = True

  while more_pages:
      if next_page_token is None:
          more_pages = False
      else:
          request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
          response = request.execute()

          for i in range(len(response['items'])):
              video_ids.append(response['items'][i]['contentDetails']['videoId'])

          next_page_token = response.get('nextPageToken')

  return video_ids


def get_video_detail(youtube, video_id):

        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {
                'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt','channelId'],
                'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount'],
                'contentDetails': ['duration', 'definition', 'caption']
            }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        if k == 'contentDetails' and v == 'duration':
                            video_info[v] = format_duration(video[k][v])
                        else:
                            video_info[v] = video[k][v]
                    except KeyError:
                        video_info[v] = None
        return (video_info)


def get_comments_in_videos(youtube, video_id):
    all_comments = []
    try:   
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()
    
        for item in response['items']:
            data={'comment_id':item['snippet']['topLevelComment']['id'],
                  'comment_txt':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                  'videoId':item['snippet']['topLevelComment']["snippet"]['videoId'],
                  'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                  'published_at':item['snippet']['topLevelComment']['snippet']['publishedAt'],
            }
            all_comments.append(data)
          
    except: 
        return 'Could not get comments for video '
    
    return all_comments

client = pymongo.MongoClient('mongodb+srv://i_am_stephen:astephen@cluster0.1pdmjhf.mongodb.net/?retryWrites=true&w=majority')
db = client['youtube_Data']

col=db["Channels"]

@st.cache_data

def channel_Details(channel_id):
  det=get_channel_sts(youtube,channel_id)
  col=db["Channels"]
  col.insert_one(det)
  playlist=get_playlists(youtube,channel_id)
  col=db["playlists"]
  for i in playlist:
    col.insert_one(i)
  Playlist=det.get('playlistId')
  videos=get_video_ids(youtube, Playlist)
  for i in videos:
    v=get_video_detail(youtube, i)
    col=db["videos"]
    col.insert_one(v)
    c=get_comments_in_videos(youtube, i)
    if c!='Could not get comments for video ':
      for j in c:
        col=db["comments"]
        col.insert_one(j)
  return ("process for a channel is completed")



def channels_table():

    try:
        cursor.execute('''create table if not exists channels(channelName varchar(50),
                   channelId varchar(80), 
                   subscribers bigint, 
                   views bigint,
                   totalVideos int,
                   playlistId varchar(80), 
                   channel_description text, 
                   primary key (channelId))'''
                   )
        mb.commit()
    except:
        mb.rollback()

    db=client["youtube_Data"]
    col=db["Channels"]
    data=col.find()
    doc=list(data)
    df=pd.DataFrame(doc)
    try:
        for _, row in df.iterrows():
            insert_query = '''
                INSERT INTO channels (channelName, channelId, subscribers, views, totalVideos, playlistId, channel_description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['channelName'],
                row['channelId'],
                row['subscribers'],
                row['views'],
                row['totalVideos'],
                row['playlistId'],
                row['channel_description']
            )
            try:
                cursor.execute(insert_query,values)
                mb.commit()
            except:
                mb.rollback()
    except:
        st.write("values already exists in the channel table")
        

def playlists_table():
    try:
        cursor.execute('''create table if not exists playlists(PlaylistId varchar(100) primary key,
                   Title text, 
                   ChannelId varchar(80), 
                   ChannelName varchar(50), 
                   PublishedAt timestamp, 
                   VideoCount int)''')
        mb.commit()
    except:
        mb.rollback()
    col=db["playlists"]
    data1=col.find()
    doc1=list(data1)
    df1=pd.DataFrame(doc1)
    try:
        for _, row in df1.iterrows():
            insert_query = '''
                INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount']
            )
            try:
                cursor.execute(insert_query,values)
                mb.commit()
            except:
                mb.rollback()
    except:
        st.write("values already exists in the playlist table")
    


def videos_table():
    try:
        cursor.execute('''create table if not exists videos(video_id varchar(50) primary key, 
                      channelTitle varchar(150), 
                      title varchar(150), 
                      description text, 
                      tags text, 
                      publishedAt timestamp, 
                      viewCount bigint, 
                      likeCount bigint,
                      favoriteCount int, 
                      commentCount int, 
                      duration interval, 
                      definition varchar(10), 
                      caption varchar(50), 
                      channelId varchar(100))''')
        mb.commit()
    except:
        mb.rollback()

    col=db["videos"]
    data4=col.find()
    doc4=list(data4)
    df4=pd.DataFrame(doc4)
    try:
        for _, row in df4.iterrows():
            insert_query = '''
                INSERT INTO videos (video_id, channelTitle,  title, description, tags, publishedAt, 
                viewCount, likeCount, favoriteCount, commentCount, duration, definition, caption, channelId)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['video_id'],
                row['channelTitle'],
                row['title'],
                row['description'],
                row['tags'],
                row['publishedAt'],
                row['viewCount'],
                row['likeCount'],
                row['favoriteCount'],
                row['commentCount'],
                row['duration'],
                row['definition'],
                row['caption'],
                row['channelId']
            )
            try:
                cursor.execute(insert_query,values)
                mb.commit()
            except:
                mb.rollback()
    except:
        st.write("values aready exists in the videos table")
    


def comments_table():
    try:
        cursor.execute('''create table if not exists comments(comment_id varchar(100) primary key, comment_txt text, 
                       videoId varchar(80), author_name varchar(150), published_at timestamp)''')
        mb.commit()
    except:
        mb.rollback()
    col3=db["comments"]
    data3=col3.find()
    doc3=list(data3)
    df3=pd.DataFrame(doc3)

    try:
        for _, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (comment_id, comment_txt, videoId, author_name, published_at)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['comment_id'],
                row['comment_txt'],
                row['videoId'],
                row['author_name'],
                row['published_at']
            )
            try:
                cursor.execute(insert_query,values)
                mb.commit()
            except:
                mb.rollback()
    except:
        st.write("values already exists in the comments table")
    
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return ("Completed!!")

def display_channels():
    db=client['youtube_Data']
    col=db['Channels']
    tableofchannels=list(col.find())
    tableofchannels=st.dataframe(tableofchannels)
    return tableofchannels
    


def display_videos():
    db=client['youtube_Data']
    col=db['videos']
    tableofvideos=list(col.find())
    tableofvideos=st.dataframe(tableofvideos)
    return tableofvideos
    


def display_playlists():
    db=client['youtube_Data']
    col=db['playlists']
    tableofplaylists=list(col.find())
    tableofplaylists=st.dataframe(tableofplaylists)
    return tableofplaylists
    

def display_comments():
    db=client['youtube_Data']
    col=db['comments']
    tableofcomments=list(col.find())
    tableofcomments=st.dataframe(tableofcomments)
    return tableofcomments
    

def one():
    try:
        cursor.execute("select title as videos, channelTitle as chanel_name from videos;")
        mb.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=['Video Title','Channel Name']))
    except:
        mb.rollback()
        cursor.execute("select title as videos, channelTitle as chanel_name from videos;")
        mb.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=['Video Title','Channel Name']))


def two():
    try:
        cursor.execute("select channelName as ChannelName,totalvideos as No_Videos from channels order by totalvideos desc limit 1;")
        mb.commit()
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=['Channel Name','No Of Videos']))
    except:
        mb.rollback()
        cursor.execute("select channelName as ChannelName,totalvideos as No_Videos from channels order by totalvideos desc limit 1;")
        mb.commit()
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=['Channel Name','No Of Videos']))

def three():
    try:
        cursor.execute('''select viewCount as views , channelTitle as ChannelName,title as Name from videos 
                        where viewCount is not null order by viewCount desc limit 10;''')
        mb.commit()
        t3=cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=['Video Views','Channel Name', 'Video Title']))
    except:
        mb.rollback()
        cursor.execute('''select viewCount as views , channelTitle as ChannelName,title as Name from videos 
                        where viewcount is not null order by viewCount desc limit 10;''')
        mb.commit()
        t3=cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=['Video Views','Channel Name', 'Video Title']))


def four():
    try:
        cursor.execute("select commentCount as No_comments ,title as Name from videos where commentCount is not null;") 
        mb.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=['No Of Comments', 'Video Title']))
    except:
        mb.rollback()
        cursor.execute("select commentCount as No_comments ,title as Name from videos where commentCount is not null;") 
        mb.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=['No Of Comments', 'Video Title']))

def five():
    try:
        cursor.execute('''select title as Video, channelTitle as ChannelName, likeCount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
        mb.commit()
        t5=cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=['Video Title', 'Channel Name','Video Likes']))
    except:
        mb.rollback()
        cursor.execute('''select title as Video, channelTitle as ChannelName, likeCount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
        mb.commit()
        t5=cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=['Video Title', 'Channel Name','Video Likes']))

def six():
    try:
        cursor.execute('''select likeCount as likes,title as Name from videos;''')
        mb.commit()
        t6=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=['Likes', 'Video title']))
    except:
        mb.rollback()
        cursor.execute('''select likeCount as likes,title as Name from videos;''')
        mb.commit()
        t6=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=['Likes', 'Video title']))

def seven():
    try:
        cursor.execute("select channelName as ChannelName, views as Channelviews from channels;")
        mb.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=['Channel Name', 'Channel Views']))
    except:
        mb.rollback()
        cursor.execute("select channelName as ChannelName, views as Channelviews from channels;")
        mb.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=['Channel Name', 'Channel Views']))

def eight():
    try:
        cursor.execute('''select title as name, publishedat as VideoRelease, channelTitle as ChannelName from videos 
                       where extract(year from publishedat) = 2022;''')
        mb.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=['Name', 'Video Publised On', 'ChannelName']))
    except:
        mb.rollback()
        cursor.execute('''select title as name, publishedat as VideoRelease, channelTitle as ChannelName from videos 
                       where extract(year from publishedat) = 2022;''')
        mb.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=['Name', 'Video Publised On', 'ChannelName']))
        
def nine():
    try:
        cursor.execute("SELECT channelTitle as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channelName;")
        mb.commit()
        t9 = cursor.fetchall()
        t9 = pd.DataFrame(t9, columns=['channelTitle', 'Average Duration'])
        T9=[]
        for _, row in t9.iterrows():
            channel_title = row['channelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))
    except:
        mb.rollback()
        cursor.execute("SELECT channelTitle as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channelName;")
        mb.commit()
        t9 = cursor.fetchall()
        t9 = pd.DataFrame(t9, columns=['channelTitle', 'Average Duration'])
        T9=[]
        for _, row in t9.iterrows():
            channel_title = row['channelTitle']
            average_duration = row['Average Duration']
            average_duration_str = str(average_duration)
            T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(T9))
        

def ten():
    try:
        cursor.execute('''select title as Name, channelTitle as ChannelName, commentCount as Comments from videos 
                       where commentcount is not null order by commentcount desc;''')
        mb.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'No Of Comments']))
    except:
        mb.rollback()
        cursor.execute('''select title as Name, channelTitle as ChannelName, commentCount as Comments from videos 
                   where commentcount is not null order by commentcount desc;''')
        mb.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'No Of Comments']))
if selected == "Home":
    # Title Image
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    st.subheader(':grey[YOUTUBE DATA HARVESTING AND WAREHOUSING]',divider='rainbow')
    channel_id = st.text_input("Enter the Channel id to collect data")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]
    st.markdown(':rainbow[Click the below button to collect the data from youtube and store it in the DB.]')
    if st.button(" Extract 📡 and Store 💻 ", type='primary'):
        for channel in channels:
            query = {'channelId': channel}
            document = col.find_one(query)
            if document:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = channel_Details(channel)
                st.success(output)

if selected == "View":
    st.markdown(':rainbow[Click the below button to migrate the data to sql tables.]')        
    if st.button("Migrate 🕹️", type='primary'):
        display=tables()
        st.success(display)
    
    

    frames = st.selectbox(
        ":rainbow[SELECT THE TABLE YOU WISH TO VIEW]",
        ('None','Channel', 'Playlist', 'Video', 'Comment'))

    if frames=='None':
        st.write("  ")
    elif frames=='Channel':
        display_channels()
    elif frames=='Playlist':
        display_playlists()
    elif frames=='Video':
        display_videos()
    elif frames=='Comment':
        display_comments()

    query = st.selectbox(
        ':rainbow[LET US DO SOME ANALYSIS]',
        ('None','1. What are the names of all the videos and their corresponding channels?', '2. Which channels have the most number of videos, and how many videos do they have?', '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?','5. Which videos have the highest number of likes, and what are their corresponding channel names?', '6. What is the total number of likes for each video, and what are their corresponding video names?', '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?','9. What is the average duration of all videos in each channel, and what are their corresponding channel names?', '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))

    if query=='None':
        st.write("")
    elif query=='1. What are the names of all the videos and their corresponding channels?':
        one()
    elif query=='2. Which channels have the most number of videos, and how many videos do they have?':
        two()
    elif query=='3. What are the top 10 most viewed videos and their respective channels?':
        three()
    elif query=='4. How many comments were made on each video, and what are their corresponding video names?':
        four()
    elif query=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        five()
    elif query=='6. What is the total number of likes for each video, and what are their corresponding video names?':
        six()
    elif query=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
        seven()
    elif query=='8. What are the names of all the channels that have published videos in the year 2022?':
        eight()
    elif query=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        nine()
    elif query=='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        ten()