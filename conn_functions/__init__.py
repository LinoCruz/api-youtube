from googleapiclient.discovery import build
from dateutil import parser
import pandas as pd
from IPython.display import JSON


def get_channel_stats(youtube, channel_ids):
    """
    It takes a list of channel ids and returns a dataframe with the channel name, number of suscribers,
    number of views, number of videos and the playlist id of the channel
    
    :param youtube: The youtube object that we created earlier
    :param channel_ids: The channel ID of the channel you want to get the data from
    :return: A dataframe with the channel name, number of suscribers, number of views, total number of
    videos and the playlist id.
    """
    all_data = []   
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids) # Id of the channel to get the data from
    )
    response = request.execute()

# Loop through the channels
    for item in response['items']:
        data = {'channelName': item['snippet']['title'],
                'suscribers': item['statistics']['subscriberCount'],
                'views': item['statistics']['viewCount'],
                'totalVideos': item['statistics']['videoCount'],
                'playlistId': item['contentDetails']['relatedPlaylists']['uploads']}
        
        all_data.append(data)
        
    return(pd.DataFrame(all_data))



def get_video_ids(youtube, playlist_id):
    """
    It takes a YouTube playlist ID and returns a list of all the video IDs in that playlist.
    
    :param youtube: the youtube object created by the get_authenticated_service() function
    :param playlist_id: The ID of the playlist you want to get the video IDs from
    :return: A list of video IDs
    """
    video_ids = []
    
    request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50
        )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        
        next_page_token = response.get('nextPageToken')
        
    return(video_ids)

def get_video_details(youtube, video_ids):
    """
    It takes a list of video ids and returns a dataframe with the details of the videos
    
    :param youtube: the youtube object that we created earlier
    :param video_ids: The list of video ids that you want to get the data from
    :return: A dataframe with the video_id, channelTitle, title, description, tags, publishedAt,
    viewCount, likeCount, dislikeCount, favouriteCount, commentCount, duration, definition, and caption.
    """
    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(video_ids[i: i+50]) # Id of the video to get the data from
            )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description','tags', 'publishedAt'],
                            'statistics' : ['viewCount', 'likeCount', 'dislikeCount', 'favouriteCount', 'commentCount'],
                            'contentDetails': ['duration', 'definition', 'caption']}
            
            video_info = {}
            video_info['video_id'] = video['id']
            
            for k in stats_to_keep.keys():
                for stat in stats_to_keep[k]:
                    try:
                        video_info[stat] = video[k][stat]
                    except:
                        video_info[stat] = None
            
            all_video_info.append(video_info)
        
    return pd.DataFrame(all_video_info) 


def get_comments_in_videos(youtube, video_ids):
    """
    Get top level comments as text from all videos with given IDs (only the first 10 comments due to quote limit of Youtube API)
    Params:
    
    youtube: the build object from googleapiclient.discovery
    video_ids: list of video IDs
    
    Returns:
    Dataframe with video IDs and associated top level comment in text.
    
    """
    all_comments = []
    
    for video_id in video_ids:
        try:   
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()
        
            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments': comments_in_video}

            all_comments.append(comments_in_video_info)
            
        except: 
            # When error occurs - most likely because comments are disabled on a video
            print('Could not get comments for video ' + video_id)
        
    return pd.DataFrame(all_comments)   