import gc
import pandas as pd
import os
import time
import spacy
import openai
from transformers import pipeline
from utilities import *
from config import *

def ner_title_extract_orgs(title): 
    doc = nlp(title)
    return [ent.text.lower() for ent in doc.ents if ent.label_ == 'ORG']

def process_sentiment_data(base_dir, folders_only, year_string, month_string, day_string, all_symbols):
    for folder in folders_only:
        for str_type in ['hot', 'top']:
            s_file = f"{base_dir}/{folder}/{str_type}/year={year_string}/month={month_string}/day={day_string}/posts.csv"
            c_file = f"{base_dir}/{folder}/{str_type}/year={year_string}/month={month_string}/day={day_string}/comments.csv"
            u_file = f"{base_dir}/{folder}/{str_type}/year={year_string}/month={month_string}/day={day_string}/sentiment.csv"

            if os.path.exists(u_file):
                print(f"sentiment file already exists: {u_file}")
                continue

            print(f"Processing files for {folder}/{str_type}")
            print(f"Posts file: {s_file}")
            print(f"Comments file: {c_file}")
            print(f"Sentiment file: {u_file}")

            stocks_posts = pd.read_csv(s_file)
            stocks_comments = pd.read_csv(c_file)

            print("Enriching dataframe with AI and NLP")
            stocks_posts.drop(columns=['url', 'subreddit', 'author'], inplace=True)
            stocks_posts['matching_symbols'] = stocks_posts['title'].apply(lambda x: find_stock_symbols_in_title(x, all_symbols['Symbol']))
            stocks_posts['matching_orgs_spacy'] = stocks_posts['title'].apply(lambda x: ner_title_extract_orgs(x))
            stocks_posts['matching_symbols_ai'] = stocks_posts['title'].apply(lambda x: query_openai_api(x, variables["OPENAI_API_KEY"]))
            stocks_posts['matching_orgs_nltk'] = stocks_posts['title'].apply(lambda x: nltk_extract_symbols(x))
            stocks_posts['final_symbols'] = stocks_posts.apply(lambda row: extract_symbols_from_df_row(row, all_symbols), axis=1)
            verify_posts_and_comments_unique(stocks_posts, stocks_comments)

            print("Getting sentiment score")
            stocks_posts['pos_neu_neg'] = stocks_posts.apply(lambda row: get_sentiment_score(row, stocks_comments, distilled_student_sentiment_classifier), axis=1)
            verify_posts_and_comments_unique(stocks_posts, stocks_comments)

            symb_sent = stocks_posts[['final_symbols', 'pos_neu_neg']]

            counter = {
                'total': {
                    'counter': 0,
                    'pos_tag': 0,
                    'neu_tag': 0,
                    'neg_tag': 0
                }
            }

            for _, row in symb_sent.iterrows():
                counter['total']['counter'] += 1
                counter['total']['pos_tag'] += row['pos_neu_neg'][0]
                counter['total']['neu_tag'] += row['pos_neu_neg'][1]
                counter['total']['neg_tag'] += row['pos_neu_neg'][2]

                if len(row['final_symbols']) <= 3:
                    for symbol in row['final_symbols']:
                        if symbol not in counter:
                            counter[symbol] = {
                                "counter": 1,
                                'pos_tag': row['pos_neu_neg'][0],
                                'neu_tag': row['pos_neu_neg'][1],
                                'neg_tag': row['pos_neu_neg'][2]
                            }
                        else:
                            counter[symbol]['counter'] += 1
                            counter[symbol]['pos_tag'] += row['pos_neu_neg'][0]
                            counter[symbol]['neu_tag'] += row['pos_neu_neg'][1]
                            counter[symbol]['neg_tag'] += row['pos_neu_neg'][2]

            df = pd.DataFrame(counter)
            df_t = df.transpose()
            df_t.sort_values(by='counter', ascending=False, inplace=True)

            print(f"Saving sentiment data to {u_file}")
            df_t.to_csv(u_file, index=True)

            print("Cleaning up dataframes")
            del [stocks_posts, stocks_comments, symb_sent, counter, df, df_t]
            gc.collect()

def combine_sentiment_data(base_dir, folders_only, year_string, month_string, day_string):
    dfs = []
    
    for folder in folders_only:
        for str_type in ['hot', 'top']:
            u_file = f"{base_dir}/{folder}/{str_type}/year={year_string}/month={month_string}/day={day_string}/sentiment.csv"
            print(f"Reading {u_file}")

            if os.path.exists(u_file):
                df = pd.read_csv(u_file)
                df.rename(columns={df.columns[0]: "symbol"}, inplace=True)
                dfs.append(df)

    if dfs:
        combined_df = pd.concat(dfs).groupby('symbol', as_index=False).sum()
        combined_df.sort_values(by='pos_tag', ascending=False, inplace=True)
        
        output_file = f'data/combined_df_year={year_string}_month={month_string}_day={day_string}.csv'
        print(f"Saving combined data to {output_file}")
        combined_df.to_csv(output_file, index=False)
        
        return combined_df
    return None

def main():
    # Load models and setup
    print("Loading spaCy model...")
    nlp = spacy.load("en_core_web_trf")
    
    variables = GetEnvironmentVariables()
    openai.api_key = variables["OPENAI_API_KEY"]
    print("OpenAI API key loaded")

    print("Loading sentiment classifier...")
    distilled_student_sentiment_classifier = pipeline(
        model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", 
        top_k=None
    )

    # Setup paths and dates
    base_dir = os.getcwd() + '/data'
    all_items = os.listdir(base_dir)
    folders_only = [item for item in all_items if os.path.isdir(os.path.join(base_dir, item))]
    year_string, month_string, day_string = time.strftime('%Y/%m/%d').split('/')

    # Load stock symbols
    print("Reading stock symbols...")
    all_symbols = pd.read_csv('data/all_stock_symbols.csv')

    # Process sentiment data
    process_sentiment_data(base_dir, folders_only, year_string, month_string, day_string, all_symbols)
    
    # Combine and save results
    combined_df = combine_sentiment_data(base_dir, folders_only, year_string, month_string, day_string)
    
    if combined_df is not None:
        print("Processing complete. Combined data saved.")
    else:
        print("No data was processed.")

if __name__ == "__main__":
    main() 