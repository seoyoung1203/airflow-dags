from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # 텍스트 감정분석 
import json
import pandas as pd

def analyze_sentiment(comment):
    analyzer = SentimentIntensityAnalyzer()
    result = analyzer.polarity_scores(comment)
    return result

# result = analyze_sentiment('i like you')
# print(result)

def convert_json_to_csv():
    hdfs_json_path = '/input/yt-data'
    hdfs_csv_path = '/input/yt-data-csv'

    from hdfs import InsecureClient
    client = InsecureClient('http://localhost:9870', user='ubuntu')

    json_files = client.list(hdfs_json_path)

    for json_file in json_files:
        json_file_path = f'{hdfs_json_path}/{json_file}'

        with client.read(json_file_path) as reader:
            data = json.load(reader) # 하둡에 저장되어있는(json)을 파이썬으로 쓸 수 있는 확장자로

        csv_data = []

        for video_id, comments in data['all_comments'].items():
            for comment in comments:
                text = comment['text']
                sentiment = analyze_sentiment(text)
                csv_data.append({
                    'video_id': video_id,
                    # 'text': text,
                    'positive': sentiment['pos'],
                    'negative': sentiment['neg'],
                    'neutral': sentiment['neu'],
                    'compound': sentiment['compound'],
                    'likeCount': comment['likeCount'],
                    'author': comment['author'] 
                })

        df = pd.DataFrame(csv_data)

        json_file_name = json_file.split('.')[0]
        csv_file_name = f'{json_file_name}.csv'
        csv_file_path = f'{hdfs_csv_path}/{csv_file_name}'

        with client.write(csv_file_path, encoding='utf-8', overwrite=True) as writer:
            df.to_csv(writer, index=False, encoding='utf-8')

convert_json_to_csv()
