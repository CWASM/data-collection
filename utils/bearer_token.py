import os

import click

LOG_STRING = click.style("SAME Tweets", fg="blue", bold=True)


def get_bearer_token():
    bt = os.environ.get("TWITTER_BEARER_TOKEN", None)
    if not bt:
        prompt = (
            f"{LOG_STRING}: Paste your Twitter Bearer Token "
            f"(see https://developer.twitter.com/en/docs/authentication/oauth-2-0/bearer-tokens) "
            f"and hit enter, or press ctrl+c to quit. "
            f"You can also set the environment variable `TWITTER_BEARER_TOKEN` to "
            f"avoid manually pasting the bearer token each time."
        )
        bt = click.prompt(prompt, hide_input=True, err=True)
    return bt


BEARER_TOKEN = get_bearer_token()
