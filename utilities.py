import csv
import os
import time
import re
import stock_types
import string
import openai
import json
import pandas as pd
from config import *
import spacy
import openai
import nltk
from nltk import word_tokenize, pos_tag, ne_chunk
from transformers import pipeline

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

def normalize_company_name(name):
    """
    Normalizes a company name by removing common prefixes and suffixes
    and returns only the first two words
    """
    # Convert to lowercase for comparison
    name = name.lower().strip()
    
    # Remove punctuation (except & and .) and numbers
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r'\d+', '', name)
    
    # Split into words
    words = name.split()
    
    # Remove common prefixes/suffixes
    words = [w for w in words if w not in stock_types.COMMON_PREFIXES]
    
    # Return only first two words
    return ' '.join(words[:2])

def remove_common_punctuation(name):
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r'\d+', '', name)
    return name.lower()

def get_company_name(name):
    """
    Extracts the company name from a given string
    """
    return normalize_company_name(name)

def query_openai_api(prompt, api_key):
    # Set your OpenAI API key
    client = openai.OpenAI(api_key=api_key)
    
    full_prompt = '''
You are a text-parsing assistant with specialized financial knowledge. Identify up to 3 stock ticker symbols in the text, along with the associated company names:

1. **Tickers**: Look for 1–5 uppercase letters (optionally with “.” and extra letters, e.g. BRK.A).
2. **Company**: If the name is explicit or well-known, provide it; if inferred from general knowledge, label it “(inferred)”; if unknown, use “Unknown.”

3. **Output**: Return a JSON array of objects
[
  { "symbol": "...", "company": "..." }
]
or [] if none found.

4. **Rules**:
- Return each valid symbol only once (no duplicates).
- Ignore irrelevant or invalid strings.
- Respect share-class suffixes (e.g., .A, .B).

Text to analyze:
'''
    
    try:
        # Call the OpenAI API using the gpt-3.5-turbo model
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "assistant", "content": full_prompt},
                {"role": "user", "content": prompt}
            ],
            max_tokens=100,  # Adjust based on expected response length
            temperature=0.1
        )
        
        # Extract the text from the response
        response_text = response.choices[0].message.content
        
        client.close()
        return get_symbols_from_query(response_text)
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return ""
    
def get_symbols_from_query(apiquery):
    symbols = []
    resp = json.loads(apiquery)

    if isinstance(resp, list):
        for item in resp:
            if isinstance(item, dict) and 'symbol' in item:
                symbols.append(item['symbol'].lower())
    else:
        if 'symbol' in resp:
            symbols.append(resp['symbol'].lower())
    return symbols

def find_stock_symbols_in_title(title, all_symbols):
    # Replace punctuation with spaces in the title
    title_no_punct = title.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
    
    # Split the title into words
    words = title_no_punct.lower().split()
     
    # Get the list of stock symbols
    stock_symbols = all_symbols.tolist()
    
    # Find matching symbols
    matching_symbols = [word for word in words if word in stock_symbols]
    
    return matching_symbols

def verify_posts_and_comments_unique(stocks_posts, stocks_comments):
    print("number of posts: ", len(stocks_posts))
    if len(stocks_posts['post_name'].unique()) == len(stocks_comments['post_name'].unique()):
        print("number of unique posts is the same in stock_posts and stock_comments")
    else:
        print("number of unique posts is not the same in stock_posts and stock_comments")
    print("number of comments: ", len(stocks_comments))

def extract_symbols_from_df_row(row, all_symbols):
    symbols = row['matching_symbols']
    matches = {}

    for symbol in symbols:
        if all_symbols['Symbol'].str.fullmatch(symbol).any():
            if matches.get(symbol) is None:
                matches[symbol] = 1
            else:
                matches[symbol] += 1

    orgs_spacy = row['matching_orgs_spacy']
    for org in orgs_spacy:
        df_matches = all_symbols[all_symbols['Security Name'].fillna('').str.startswith(org)]
        for _, r in df_matches.iterrows():
            if matches.get(r['Symbol']) is None:
                matches[r['Symbol']] = 1
            else:
                matches[r['Symbol']] += 1  # Corrected from matches[row['Symbol']]

    symbols_openai = row['matching_symbols_ai']
    for symbol in symbols_openai:
        if all_symbols['Symbol'].str.fullmatch(symbol).any():
            if matches.get(symbol) is None:
                matches[symbol] = 1
            else:
                matches[symbol] += 1
    
    orgs_nltk = row['matching_orgs_nltk']
    for org in orgs_nltk:
        df_matches = all_symbols[all_symbols['Security Name'].fillna('').str.startswith(org)]
        for _, r in df_matches.iterrows():
            if matches.get(r['Symbol']) is None:
                matches[r['Symbol']] = 1
            else:
                matches[r['Symbol']] += 1  # Corrected from matches[row['Symbol']]

    return [symbol for symbol, count in matches.items() if count >= 2]

def get_sentiment_score(row, stocks_comments, distilled_student_sentiment_classifier):
    positive, neutral, negative = 0, 0, 0

    post_comments_rows = stocks_comments[stocks_comments['post_name'] == row['post_name']]
    if post_comments_rows.empty:
        return 0, 0, 0


    try:
        for _, comment_row in post_comments_rows.iterrows():
            body = comment_row['body']
            sentiment_scores = distilled_student_sentiment_classifier(body, truncation=True, max_length=512)
            for dict in sentiment_scores[0]:
                if dict['label'] == 'negative':
                    negative += comment_row['score'] * dict['score']
                elif dict['label'] == 'neutral':
                    neutral += comment_row['score'] * dict['score']
                elif dict['label'] == 'positive':
                    positive += comment_row['score'] * dict['score']
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Error message: {str(e)}")
        return 0, 0, 0

    return [int(positive), int(neutral), int(negative)]

def nltk_extract_symbols(sentence):
    tokens = word_tokenize(sentence)
    pos_tags = pos_tag(tokens)
    entities = ne_chunk(pos_tags)

    proper_nouns = [word.lower() for word, pos in pos_tags if pos == 'NNP']
    return proper_nouns