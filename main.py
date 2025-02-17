from config import *
from apiclient import RedditClient
import logging
from utilities import *

def main():
    user_vars = GetEnvironmentVariables()

    # handler = logging.StreamHandler()
    # handler.setLevel(logging.DEBUG)
    # for logger_name in ("praw", "prawcore"):
    #     logger = logging.getLogger(logger_name)
    #     logger.setLevel(logging.DEBUG)
    #     logger.addHandler(handler)

    client = RedditClient(user_vars)
    subscriptions = client.get_subscriptions()
    print(subscriptions)
    sort_type = ["hot", "top"]
    
    for sub in subscriptions:
        for sort in sort_type:
            print("Top threads for " + sub.display_name + " in the last day:")
            posts = client.get_subreddit_posts(sub, "day", 100, sort)
            save_posts_to_csv(sub, sort, posts)

            for post in posts:
                print(post.title)
                comments = client.get_post_comments(post)
                save_comments_to_csv(sub, sort, comments, post.name)

if __name__ == "__main__":
    main()