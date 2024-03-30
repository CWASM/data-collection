import math
import re
from tqdm import tqdm

from typing import List, Dict, Callable

WarningT = str
TweetT = str
TokenizerT = Callable[[str], List[str]]


hashtag_regex = r'\B#\w*[a-zA-Z]+\w*' # https://stackoverflow.com/a/54147208
tokenizers: Dict[str, TokenizerT] = {
    'ws': lambda x: x.split(),
    'hashtag': lambda x: re.findall(hashtag_regex, x),
}
default_tokenizer = tokenizers['hashtag']


def tf_iwf(tweets: Dict[WarningT, List[TweetT]],
           tokenizer: TokenizerT = default_tokenizer):
    """ Term frequency - inverse warning frequency

    tf: frequency of a term in a set of tweets containing a given warning
    iwf: inverse proportion of warnings that contain a given term

    tf_iwf = tf * log(iwf)

    Not very suitable for small number of warnings, as tf dominates the equation
    """
    tf_dict = {}
    tf_total = {w: 0 for w in tweets}
    for warning, tweets in tweets.items():
        tf_dict[warning] = {}
        for tweet in tqdm(tweets, desc=f"Iterating over tweets for {warning}"):
            for token in tokenizer(tweet):
                tf_dict[warning][token] = tf_dict[warning].get(token, 0) + 1
                tf_total[warning] += 1

    tf_iwf_dict = {}
    N = len(tf_dict)
    for warning, tfs in tf_dict.items():
        tf_iwf_dict[warning] = {}
        for token, tf in tfs.items():
            tf = tf / tf_total[warning]
            # number of warnings (documents) containing token
            n_t = sum([1 for _, _tfs in tf_dict.items() if token in _tfs])
            iwf = N / n_t
            tf_iwf_dict[warning][token] = tf * math.log(iwf)
        tf_iwf_dict[warning] = {
            k: v for k, v in sorted(
                tf_iwf_dict[warning].items(),
                reverse=True,
                key=lambda x: x[1]
            )
        }
    return tf_iwf_dict


def wtf(tweets: Dict[WarningT, List[TweetT]],
           tokenizer: TokenizerT = default_tokenizer):
    """WTF term frequency. """
    # tweet frequency (how many tweets does a term appear in)
    tf_dict = {}
    for warning in tweets:
        tf_dict[warning] = {}
        for tweet in tweets[warning]:
            for token in set(tokenizer(tweet)):
                tf_dict[warning][token] = tf_dict[warning].get(token, 0) + 1

    wtf_dict = {}
    for warning, tfs in tf_dict.items():
        wtf_dict[warning] = {}
        for token, tf in tfs.items():
            # how specific a term is to a given warning
            # (based on tweet frequency, not just occurrence as in tf-iwf)
            specificity = tf / sum([tf_dict[w].get(token, 0) for w in tf_dict])
            # rescale from [0,1] to [-1, 1]
            specificity = 2 * (specificity - 0.5)  # [-1,1]
            # how representative a term is of a given warning
            # (based on token frequency relative to number of tweets,
            # not token frequency relative to other tokens as in tf-iwf)
            representativity = tf / len(tweets[warning])
            wtf_dict[warning][token] = specificity * representativity

        wtf_dict[warning] = {
            k: v for k, v in sorted(
                wtf_dict[warning].items(),
                reverse=True,
                key=lambda x: x[1]
            )
        }
    return wtf_dict


