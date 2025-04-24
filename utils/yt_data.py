# pip install google-api-python-client

from googleapiclient.discovery import build
from dotenv import load_dotenv # .env 파일에 저장된 환경 변수들을 파이썬 코드에서 사용
import os # 운영체제와 상호작용
from pprint import pprint 
from hdfs import InsecureClient
import json
from datetime import datetime

load_dotenv('/home/ubuntu/airflow/.env')
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# api 발급 받고 사용할 API의 이름, 사용할 API 버전
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY) # build 구글 api 사용할게 함수
# print(youtube)

# handle을 기준으로 channelID 리턴하는 함수
def get_channel_id(youtube, handle):
    response = youtube.channels().list(part='id', forHandle=handle).execute() # part 내가 보고싶은 컬럼(에 대한 결과를 지정)
    return response['items'][0]['id'] # tab으로 나눠서 볼 수 있게끔 출력, list 형태이기 때문에 index 접근

# channelid 만들기
target_handle = 'triplegdot'
channel_id = get_channel_id(youtube, target_handle)
# pprint(channel_id)

# channel id를 기준으로 최신 영상들의 id들을 리턴하는 함수
def get_latest_video_ids(youtube, channel_id):
    response = youtube.search().list(
        # part='snippet',
        part='id',
        channelId= channel_id,
        maxResults=5, # 최근 5개 영상을 불러올게
        order='date', # 날짜 기준으로 정렬된 영상 불러올게
    ).execute()

    video_ids = []

    for item in response['items']:
        video_ids.append(item['id']['videoId'])

    return video_ids

latest_video_ids = get_latest_video_ids(youtube, channel_id)
# print(latest_video_ids)


#video_Id를 기준으로 comment를 return 하는 함수
def get_comments(youtube, video_id):
    response = youtube.commentThreads().list(
        part='snippet', # 보고 싶은 속성
        videoId=video_id, 
        textFormat='plainText', # 몇 개 데이터 줄지
        maxResults=100,
        order='relevance',
    ).execute()

    comments = []

    for item in response['items']:
        comment = {
            'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
            'publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt'],
            'likeCount': item['snippet']['topLevelComment']['snippet']['likeCount'],
            'commentId': item['snippet']['topLevelComment']['id']
        }
        comments.append(comment)
    return comments

# 구현한 함수를 실행하여 최종 형태 리턴
def get_handle_to_comments(youtube, handle):
    channel_id = get_channel_id(youtube, handle)
    latest_video_ids = get_latest_video_ids(youtube, channel_id)

    all_comments = {}



    for video_id in latest_video_ids: # 비디오 하나에 따라 댓글 수집 ~ ~
        comments = get_comments(youtube, video_id)
        all_comments[video_id] = comments

        return{
            'handle': handle,
            'all_comments': all_comments
        }
    
# get_handle_to_comments(youtube, target_handle)

def save_to_hdfs(data, path):
    
    client = InsecureClient('http://localhost:9870', user='ubuntu')

    current_date = datetime.now().strftime('%y%m%d%H%M')
    file_name = f'{current_date}.json' # 최종적으로 저장할 파일의 이름

    hdfs_path = f'{path}/{file_name}'

    json_data = json.dumps(data, ensure_ascii=False) # json으로 바꾸기

    with client.write(hdfs_path, encoding='utf-8') as writer:
        writer.write(json_data)

# data = get_handle_to_comments(youtube, target_handle)
# save_to_hdfs(data, '/input/yt-data')

