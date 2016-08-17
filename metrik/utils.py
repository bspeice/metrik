import requests
from metrik.conf import USER_AGENT


def masked_get(url, extra_headers=None):
    user_header = {'User-Agent': USER_AGENT}
    if extra_headers is not None:
        user_header.update(extra_headers)
    return requests.get(url, headers=user_header)
