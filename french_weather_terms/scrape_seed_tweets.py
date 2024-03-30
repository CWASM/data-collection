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

start_time="2022-01-01T00:00:00.000Z" # start of the month
end_time="2022-02-01T00:00:00.000Z" # end of the month

# Here we build the seed query composed of SAME warnings
# relevant to the event (NOTE: these are event=specific and should be adapted to each event).
french_weather = [
    "neige",
    "tempete",
    "verglas",
    "verglacante",
    "bourrasques",
    "vents forts",
    "rafales",
    "froid extrême",
]
query = ' OR '.join([f'"{x}"' for x in french_weather])

# we also specify not to include retweets
query = f"({query}) -is:retweet"

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

scraping_dir = DATA_DIR / "2022_01_french_weather_terms/raw"
scraping_dir.mkdir(exist_ok=True, parents=True)

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