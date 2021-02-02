import nltk
# nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk.data
tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
sid = SentimentIntensityAnalyzer()
sid.polarity_scores(post.body)



# goal: get sentiment and ticker extraction

def get_sentiment(text, deep_dive=True):
    document_sentiment = sid.polarity_scores(text)
    if deep_dive:
        for word in tokenizer.tokenize(text):
