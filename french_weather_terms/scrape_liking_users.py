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
liking_user_dir = scraping_dir / "liking_users"
liking_user_dir.mkdir(exist_ok=True)

user_fields = [
  "verified",
  "created_at",
  "description",
  "location",
  "protected",
  "public_metrics",
]

num_retry = 3

already_searched_tweets = set()
for user_json in tqdm(list(liking_user_dir.glob("*.json"))):
  user = json.loads(user_json.read_text())
  for tweet_id in user['_source_tweet_id']:
    already_searched_tweets.add(tweet_id)

for tweet_json in tqdm(list(seed_tweet_dir.glob("*.json"))):
  tweet = json.loads(tweet_json.read_text())
  tweet_id = tweet['id']
  if  tweet_id in already_searched_tweets:
    continue

  elif tweet['public_metrics']['like_count']:
      glu_kwargs = dict(
        id=tweet_id,
        user_fields=user_fields
      )
      for attempt in range(num_retry):
        try:
          for response in tweepy.Paginator(client.get_liking_users, **glu_kwargs):
            sleep(1)
            for user in response.data or []:
              json_data = user.data
              user_path = liking_user_dir / f"{json_data['id']}.json"
              if user_path.exists():
                json_data = json.loads(user_path.read_text())
              else:
                json_data['_source_tweet_id'] = []
              json_data['_source_tweet_id'].append(tweet_id)
              user_path.write_text(json.dumps(json_data, indent=2))
        except Exception as e:
          if attempt < num_retry:
            client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)
            continue
          else:
            raise e
        else:
          break