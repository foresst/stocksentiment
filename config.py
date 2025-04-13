import os
from dotenv import load_dotenv

def GetEnvironmentVariables():
    load_dotenv()
    
    variables = {
        "client_id": os.getenv("REDDIT_CLIENT_ID"),
        "client_secret": os.getenv("REDDIT_CLIENT_SECRET"),
        "user_agent": os.getenv("REDDIT_USER_AGENT"),
        "username": os.getenv("REDDIT_USERNAME"),
        "password": os.getenv("REDDIT_PASSWORD"),
        "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY")
    }
    return variables

def DefaultRedditCrawlerConfig():
    config = {
        "Length": "day",
        "NumberOfThreads": 100,
        "PageSize": 100,
        "CommentCount": 500,
        "Subfilter": ["top", "hot"]
    }
    return config  

def NewRedditCrawlerConfig(length, numberOfThreads, pageSize, commentCount, subfilter):
    config = {
        "Length": length,
        "NumberOfThreads": numberOfThreads,
        "PageSize": pageSize,
        "CommentCount": commentCount,
        "Subfilter": subfilter
    }
    return config  # Added return statement to return the config
