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
seed_tweet_dir = scraping_dir / "seed_tweets"
seed_user_dir = scraping_dir / "seed_users"
seed_user_dir.mkdir(exist_ok=True)

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

user_fields = [
  "verified",
  "created_at",
  "description",
  "location",
  "protected",
  "public_metrics",
]

num_retry = 3

already_searched_users = set()
for user_json in tqdm(list(seed_user_dir.glob("*.json"))):
  author_id = user_json.stem
  already_searched_users.add(author_id)

user_queue = []
for tweet_json in tqdm(list(sorted(seed_tweet_dir.glob("*.json")))):
  tweet = json.loads(tweet_json.read_text())
  author_id = tweet['author_id']
  if  author_id in already_searched_users:
    continue
  # batch queries to max of 100 ids to reduce API overhead
  elif len(user_queue) < 100:
    user_queue.append(author_id)
    already_searched_users.add(author_id)
  else:
      glu_kwargs = dict(
        ids=user_queue,
        user_fields=user_fields
      )
      user_queue = []
      for attempt in range(num_retry):
        try:
          response = client.get_users(**glu_kwargs)
          sleep(1)
          for user in response.data or []:
            json_data = user.data
            user_path = seed_user_dir / f"{json_data['id']}.json"
            user_path.write_text(json.dumps(json_data, indent=2))
        except Exception as e:
          if attempt < num_retry:
            client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)
            continue
          else:
            raise e
        else:
          break