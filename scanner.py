from secret_reddit_instance import reddit as r
import praw
from praw.models.reddit.submission import Submission
from praw.models.reddit.comment import Comment
import pandas as pd
import numpy as np
from prettytable import PrettyTable
import time
from datetime import datetime


R = "\033[1;31m"  # RED
G = '\033[1;32m'  # GREEN
Y = "\033[1;33m"  # Yellow
B = "\033[1;34m"  # Blue
N = "\033[0m"  # Reset


def submissions_and_comments(subreddit, **kwargs):
    results = []
    results.extend(subreddit.new(**kwargs))
    results.extend(subreddit.comments(**kwargs))
    results.sort(key=lambda post: post.created_utc, reverse=True)
    return results


if __name__ == '__main__':
    start_time = time.time()
    try:
        info_data = pd.read_csv('info_data.csv', index_col=0)
        print('lets gooooooo!')
    except:
        print('first time, eh?')  # it will always say that if you dont change the file name
        info_data = pd.DataFrame(
            columns=['type', 'id', 'title', 'text', 'date', 'votes_ratio', 'url', 'author'])
        pass
    subreddit = r.subreddit('wallstreetbets')
    stream = praw.models.util.stream_generator(lambda **kwargs: submissions_and_comments(subreddit, **kwargs),
                                               skip_existing=True)

    for post in stream:
        if isinstance(post, Submission):
            info_data = info_data.append({'type': 'submission', 'id': post.id,
                                          'text': post.title + ' ' + post.selftext if post.selftext is not None else post.title,
                                          'date': post.created_utc, 'votes_ratio': post.upvote_ratio, 'url': post.url,
                                          'author': post.author, 'title': post.title}, ignore_index=True)

        elif isinstance(post, Comment):
            info_data = info_data.append({'type': 'comment', 'id': post.id, 'text': post.body, 'date': post.created_utc,
                                          'votes_ratio': post.score, 'url': post.submission.url, 'author': post.author},
                                         ignore_index=True)
        else:
            print('something... else')
            break
        new_time = time.time()
        if new_time - start_time > 3600:
            tab = PrettyTable()
            tab.title = f'{len(info_data)} rows, {np.round((new_time - start_time)/3600, 3)} ' \
                        f'hrs since last checkin'
            tab.field_names = ['post type', 'count', 'increase from last report']
            tab.add_row([B + 'submission' + N, len(info_data.query('type == "submission"')),
                         len(info_data.query(f'(date > {new_time}) & (type == "submission")'))])
            tab.add_row([Y + 'comment' + N, len(info_data.query('type == "comment"')),
                         len(info_data.query(f'(date > {new_time}) & (type == "comment")'))])
            print(tab)
            info_data.to_csv(f'/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/info_saves/info_data_'
                             f'({datetime.now().strftime("%Y-%m-%d %H:%M:%S")}).csv')
            info_data.to_csv(f'/Users/biscuit/Documents/GitHub/wallstreet_bets_bot/info_data.csv')
            start_time = new_time
