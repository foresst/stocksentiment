import csv
import os
import time

def save_posts_to_csv(sub, sort, data):
    dirname = make_dirname(sub, sort)
    filename = f"{dirname}posts.csv"
    print("creating file: ", filename)

    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['title', 'url', 'subreddit', 'author', 'score', 'num_comments', 'created_utc', 'name'])
        for post in data:
            writer.writerow([post.title, post.url, post.subreddit, post.author, post.score, post.num_comments, post.created_utc, post.name])

def save_comments_to_csv(sub, sort, comments, post_name):
    dirname = make_dirname(sub, sort)
    filename = f"{dirname}comments.csv"
    print("creating file: ", filename)

    with open(filename, 'a') as f:
        writer = csv.writer(f)
        if os.stat(filename).st_size == 0:
            writer.writerow(['post_name', 'body', 'author', 'score', 'created_utc', 'name'])
        for comment in comments:
            writer.writerow([post_name, comment.body, comment.author, comment.score, comment.created_utc, comment.name])
    
def make_dirname(sub, sort):
    year, month, day = time.strftime("%Y %m %d").split()
    dirname = f"data/{sub.display_name.lower()}/{sort}/year={year}/month={month}/day={day}/"
    os.makedirs(os.path.dirname(dirname), exist_ok=True)
    return dirname