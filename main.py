import pandas as pd
import os
import time
from cffi.setuptools_ext import execfile
from text_analyzer import get_sentiment_and_extract_tickers
import pytz
# sentiment_and_tickers_table = pd.DataFrame(columns=['id', 'type', 'document', 'sentence', 'company_name', 'symbol', 
#                                                     'chosen_by_symbol', 'neg', 'neu', 'pos', 'compound', 
#                                                     'date_processed', 'date_posted'])
# sentiment_and_tickers_table.iloc[:, 1:].to_csv('sentiment_and_tickers_table.csv')

if __name__ == '__main__':
    execfile('scanner.py')
    file_start_time = time.time()
    raw_stream_data = pd.read_csv('info_data.csv', index_col=0)  # this id the output of any and every post
    #                                                              (its basic info)
    sentiment_and_tickers_table = pd.read_csv('sentiment_and_tickers_table.csv', index_col=0)  # the data analysis will
    #                                                                                            be based on
    # company_chatter = pd.read_csv('company_chatter_table.csv')
    tz = pytz.timezone('America/Los_Angeles')
    last_index = raw_stream_data.index[-1]
    while True:
        if time.time() - file_start_time > 3600:
            new_data = pd.read_csv('info_data.csv')
            new_data = new_data.loc[last_index:, :]
            last_index = new_data.index[-1]
            for post in new_data.itertuples():
                text_results = get_sentiment_and_extract_tickers(post.text, True)
                if text_results.empty:
                    continue
                for row in text_results.itertuples():
                    sentiment_and_tickers_table = sentiment_and_tickers_table.append({'id': row.id, 'type': row.type,
                                                                                      'document': sentiment_and_tickers_table['document'],
                                                                                      'sentence': sentiment_and_tickers_table['sentence'],
                                                                                      'company_name': sentiment_and_tickers_table['company_name'],
                                                                                      'symbol': sentiment_and_tickers_table['symbol'],
                                                                                      'chosen_by_symbol': sentiment_and_tickers_table['chosen_by_symbol'],
                                                                                      'neg': sentiment_and_tickers_table['neg'],
                                                                                      'neu': sentiment_and_tickers_table['neu'],
                                                                                      'pos': sentiment_and_tickers_table['pos'],
                                                                                      'compound': sentiment_and_tickers_table['compound'],
                                                                                      'date_processed': sentiment_and_tickers_table['date_processed'],
                                                                                      'date_posted': pd.to_datetime(row.date, unit='s')},
                                                                                     ignore_index=True)
            print(f'sentiment_and_tickers_table is {len(sentiment_and_tickers_table)} rows long after completing '
                  f'{len(new_data)} rows')
            sentiment_and_tickers_table.to_csv(
                f'sentiment_and_tickers/sentiment_and_tickers_table_{pd.to_datetime(time.time(), unit="s")}.csv')
            sentiment_and_tickers_table.to_csv(f'sentiment_and_tickers_table.csv')
