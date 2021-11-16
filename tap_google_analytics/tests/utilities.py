"""Test utility funtions."""
import base64
import json
import os


def get_secrets_dict():
    """Return a secrets dictionary of based off the env var."""
    secrets_var = os.environ["CLIENT_SECRETS"]
    try:
        secrets_dict = json.loads(secrets_var)
    except json.JSONDecodeError:
        secrets_dict = json.loads(base64.b64decode(secrets_var))
    return secrets_dict
