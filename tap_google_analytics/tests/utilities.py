"""Test utility funtions."""
import json
import os


def get_secrets_dict():
    """Return a secrets dictionary of based off the env var."""
    return json.loads(os.environ["CLIENT_SECRETS"])
