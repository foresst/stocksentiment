import csv
import os
import time

def save_posts_to_csv(sub, sort, data):
    dirname = make_dirname(sub, sort)
    filename = f"{dirname}posts.csv"
    print("creating file: ", filename)
    header_list = ['title', 'url', 'subreddit', 'author', 'score', 'num_comments', 'created_utc', 'post_name']

    with open(filename, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=header_list)
        writer.writeheader()
        for post in data:
            writer.writerow({
                'title': post.title, 
                'url': post.url, 
                'subreddit': post.subreddit, 
                'author': post.author, 
                'score': post.score, 
                'num_comments': post.num_comments, 
                'created_utc': post.created_utc, 
                'post_name': post.name,
            })

def save_comments_to_csv(sub, sort, comments, post):
    dirname = make_dirname(sub, sort)
    filename = f"{dirname}comments.csv"
    print("creating file: ", filename)
    header_list = ['post_name', 'body', 'author', 'score', 'created_utc']

    # Check if file exists and is empty
    file_exists = os.path.exists(filename)
    file_empty = file_exists and os.path.getsize(filename) == 0

    # Open file in append mode unless it's empty
    mode = 'w' if not file_exists or file_empty else 'a'
    with open(filename, mode) as f:
        writer = csv.DictWriter(f, fieldnames=header_list)
        # Write header only for new or empty files
        if not file_exists or file_empty:
            writer.writeheader()
        # add post self text to comments
        writer.writerow({
            'post_name': post.name, 
            'body': post.selftext, 
            'author': post.author, 
            'score': post.score, 
            'created_utc': post.created_utc, 
        }) 
        for comment in comments:
            writer.writerow({
                'post_name': post.name, 
                'body': comment.body, 
                'author': comment.author, 
                'score': comment.score, 
                'created_utc': comment.created_utc, 
            })
    
def make_dirname(sub, sort):
    year, month, day = time.strftime("%Y %m %d").split()
    dirname = f"data/{sub.display_name.lower()}/{sort}/year={year}/month={month}/day={day}/"
    os.makedirs(os.path.dirname(dirname), exist_ok=True)
    return dirname

def crawl_folder(folder_path):
    folder_structure = {}

    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if os.path.isdir(item_path):
            folder_structure[item] = crawl_folder(item_path)  # Recursively crawl subdirectories
        else:
            folder_structure[item] = "file"  # You can also store more info if needed

    return folder_structure

def get_filenames(structure, current_path=''):
    filenames = []
    for item, value in structure.items():
        path = os.path.join(current_path, item)
        if value == "file":
            filenames.append(path)
        else:
            filenames.extend(get_filenames(value, path))
    return filenames
