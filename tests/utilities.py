"""Test utility funtions."""
import base64
import contextlib
import json
import os


@contextlib.contextmanager
def create_secrets_file():
    """Write secrets file then clean it up after tests."""
    secrets_path = f"{os.path.dirname(__file__)}/test_data/client_secrets.json"
    with open(secrets_path, "w") as f:
        json.dump(get_secrets_dict(), f)
    yield secrets_path
    os.remove(secrets_path)


def get_secrets_dict():
    """Return a secrets dictionary of based off the env var."""
    secrets_var = os.environ["CLIENT_SECRETS"]
    try:
        secrets_dict = json.loads(secrets_var)
    except json.JSONDecodeError:
        secrets_dict = json.loads(base64.b64decode(secrets_var))
    return secrets_dict
