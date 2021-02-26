import nltk
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
import nltk.data
import spacy
from collections import Counter
from nltk.corpus import words
import datetime
nlp = spacy.load("en_core_web_lg")
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
sid = SentimentIntensityAnalyzer()
nyse_data = pd.read_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/newest_combined_listed_csv.csv')

# nyse_data = pd.read_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/nyse-listed_csv.csv')
# nyse_data = pd.read_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/nasdaq_screener.csv')
# nyse_data2 = pd.read_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/nasdaq_screener2.csv')
# nyse_data3 = pd.read_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/nasdaq_screener3.csv')
# nyse_data_full = pd.concat([nyse_data, nyse_data2, nyse_data3], axis=0)
# nyse_data_full.to_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/newest_combined_listed_csv.csv')
# not_nyse_data = pd.read_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/other-listed_csv.csv')
# combined_data = pd.concat([nyse_data, not_nyse_data], axis=0)
# combined_data.drop_duplicates('ACT Symbol', keep='last').to_csv('/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/combined_listed_csv.csv')
# # sid.polarity_scores(post.body)



# goal: get sentiment and ticker extraction
stop_dict = {}
full_str = ' '.join(nyse_data.Name)
for word in full_str.split(' '):
    word = word.lower()
    if word is '':
        continue
    try:
        stop_dict[word] += 1
    except:
        stop_dict[word] = 1

stopwords = [word[0] for word in Counter(stop_dict).most_common(300)]

additional_add_words = ['pharma', 'silver', 'gold', 'crypto', 'bitcoin', 'dogecoin']



def get_sentiment(text, deep_dive=True):
    """

    :param text: text to gain sentiment of
    :param deep_dive: whole doc or sentence by sentence included (if true, -1 index is whole doc)
    :return: dataframe with document and sentence info. -1 index is whole doc if deep_dive is true.
    """
    document_sentiment = sid.polarity_scores(text)
    if not deep_dive:
        document_sentiment['document'] = text
        document_sentiment['sentence'] = None
        return pd.DataFrame(document_sentiment, index=[0])
    else:
        token_list = tokenizer.tokenize(text)
        individual_sentiment = pd.DataFrame(columns=['document', 'sentence', 'neg', 'neu', 'pos', 'compound'],
                                            index=range(len(token_list)))
        for i in range(len(token_list)):
            pol = sid.polarity_scores(token_list[i])
            pol['sentence'] = token_list[i]
            pol['document'] = text
            individual_sentiment.loc[i, :] = pol
        pol = sid.polarity_scores(text)
        pol['document'] = text
        pol['sentence'] = None
        individual_sentiment.loc[-1, :] = pol
        return individual_sentiment


def get_tickers_from_doc(text, deep_dive=True):
    if not deep_dive:
        doc = nlp(text)
        ret = pd.DataFrame(columns=['document', 'sentence', 'company_name', 'symbol', 'chosen_by_symbol'])
        for token in doc:
            if token.pos_ == 'PROPN' and (str(token) not in stopwords):
                bool_ticker = nyse_data['Symbol'].str.lower() == str(token).lower()
                bool_ticker_specific = any(bool_ticker)
                bool_name = nyse_data['Name'].str.lower().str.contains(" " + str(token).lower() + " ")
                bool_temp = bool_name | bool_ticker

                if any(bool_temp):
                    for row in bool_temp.iteritems():
                        if row[1]:
                            ret = ret.append({'document': text, 'sentence': None,
                                              'company_name': nyse_data.loc[row[0], 'Name'],
                                              'symbol': nyse_data.loc[row[0], 'Symbol'],
                                              'chosen_by_symbol': True if bool_ticker_specific else False},
                                             ignore_index=True)
    if deep_dive:
        token_list = tokenizer.tokenize(text)
        ret = pd.DataFrame()
        for token in token_list:
            temp = get_tickers_from_doc(token, False)
            temp['sentence'] = temp['document']
            temp['document'] = text
            ret = ret.append(temp, ignore_index=True)

    return ret.dropna(subset=['document'])


def get_sentiment_and_extract_tickers(doc, deep_dive=True):
    sentiment_analysis = get_sentiment(doc, deep_dive)
    ticker_info = get_tickers_from_doc(doc, deep_dive)
    full_data = ticker_info.merge(sentiment_analysis.drop(columns='document'), left_on='sentence', right_on='sentence', how='left')
    full_data['date_processed'] = datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S')
    return full_data


if __name__ == '__main__':
    text = 'GME is shooting to the moon! Hold fast you scurvey dogs! Apple sucks.'

    sentiment_analysis = get_sentiment(text)
    ticker_info = get_tickers_from_doc(text, False)
    ticker_info = get_tickers_from_doc(text, True)

    final_data = get_sentiment_and_extract_tickers(text)


# TODO:
#   1. add most common commodities to csv list as 'company'
#   2. write 3rd function that summarizes sentiment and tickers extracted
