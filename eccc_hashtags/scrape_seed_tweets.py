import sys
from pathlib import Path

root_dir = Path(__file__).resolve().parents[1].absolute()
sys.path.append(str(root_dir))

import json
from tqdm import tqdm
from time import sleep

import tweepy
from utils import DATA_DIR
from utils.bearer_token import BEARER_TOKEN
client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)

scraping_dir = DATA_DIR / "2021_12_canadian_weather_hashtags/raw"
scraping_dir.mkdir(exist_ok=True, parents=True)

start_time="2021-12-01T00:00:00.000Z" # start of the month
end_time="2022-02-01T00:00:00.000Z"  # end of the month

# create a list of canadian weather hashtags to search for
hashtags = []
provinces = ['on', 'qc', 'ab', 'bc', 'mb', 'nb', 'nl', 'ns', 'nt', 'nu', 'pe', 'sk', 'yt']
weather_tags = ['storm', 'wx']
for province in provinces:
    for tag in weather_tags:
        hashtags.append(f'#{province}{tag}')

query = ' OR '.join(hashtags)

# we also specify not to include retweets
query = f"({query}) -is:retweet"
print(query)

# By default, the twitter API will only return a limited amount of data
# for a given tweet. To expand what is returned, we use fields and expansions.
# see: https://docs.tweepy.org/en/v4.8.0/client.html#expansions-and-fields-parameters

# see: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
tweet_fields = [
  "author_id",
  "conversation_id",
  "created_at",
  "geo",
  "in_reply_to_user_id",
  "lang",
  "public_metrics",
  "referenced_tweets",
  "source",
  "reply_settings",
]

# see: https://developer.twitter.com/en/docs/twitter-api/expansions
expansions = [
  "author_id",
  "geo.place_id"
]

# see: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user
user_fields = [
  "verified"
]

# see: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/place
place_fields = [
  "place_type"
]

seed_tweet_dir = scraping_dir / "seed_tweets"
seed_tweet_dir.mkdir(exist_ok=True)

search_kwargs = dict(
    query=query,
    tweet_fields=tweet_fields,
    expansions=expansions,
    user_fields=user_fields,
    start_time=start_time,
    end_time=end_time,
    max_results=100
)


for response in tqdm(tweepy.Paginator(client.search_all_tweets, **search_kwargs)):
  # prevent 1 request / s rate limit to trip up 15min wait due to 300 request / 15 min rate limit
  # see: https://github.com/tweepy/tweepy/issues/1871
  sleep(1)
  for i,tweet in enumerate(response.data):
    json_data = tweet.data
    for expansion in response.includes.keys():
      # TODO: figure out how to align places with tweets
      #       in cases where places might have e.g. a list of length 1
      #       compared to the list of length 100 for tweets
      if len(response.includes[expansion]) == len(response.data):
        for k,v in response.includes[expansion][i].items():
          json_data[f"_{expansion}_{k}"] = v

    tweet_path = seed_tweet_dir / f"{json_data['id']}.json"
    tweet_path.write_text(json.dumps(json_data, indent=2))