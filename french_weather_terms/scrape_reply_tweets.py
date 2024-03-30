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
reply_dir = scraping_dir / "reply_tweets"
reply_dir.mkdir(exist_ok=True)

num_retry = 3

already_searched_tweets_p = reply_dir / "already_searched_tweets.txt"
already_searched_tweets_p.touch(exist_ok=True)
already_searched_tweets = already_searched_tweets_p.read_text().splitlines()
already_searched_tweets = set(already_searched_tweets)

for tweet_json in tqdm(list(seed_tweet_dir.glob("*.json"))):
  tweet = json.loads(tweet_json.read_text())
  conversation_id = tweet['conversation_id']

  if conversation_id in already_searched_tweets:
    continue

  # skip tweets/conversations with 0 replies
  num_replies = tweet['public_metrics']['reply_count']
  if not (conversation_id == tweet['id'] and num_replies == 0):
    reply_kwargs = dict(
      query=f"conversation_id:{conversation_id}",
      tweet_fields=tweet_fields,
      expansions=expansions,
      user_fields=user_fields,
      max_results=100,
    )
    for attempt in range(num_retry):
      try:
        for response in tweepy.Paginator(client.search_all_tweets, **reply_kwargs):
          sleep(1)
          for reply in response.data or []:
            json_data = reply.data
            reply_path = reply_dir / f"{json_data['id']}.json"
            reply_path.write_text(json.dumps(json_data, indent=2))
      except Exception as e:
        if attempt < num_retry:
          client = tweepy.Client(bearer_token=BEARER_TOKEN, wait_on_rate_limit=True)
          continue
        else:
          raise e
      else:
        break

  already_searched_tweets.add(conversation_id)
  txt = "\n".join([str(x) for x in already_searched_tweets])
  already_searched_tweets_p.write_text(txt)