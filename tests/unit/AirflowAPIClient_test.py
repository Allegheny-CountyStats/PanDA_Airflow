import os
from dotenv import load_dotenv
from PanDAirflow.api import AirflowAPIClient


def test_airflow_login_and_pools():
    """
    Unit test that loads API_USER / API_PW from .env and
    validates that the Airflow API client can authenticate
    and retrieve pool data.
    """
    # Load environment variables
    load_dotenv()

    username = os.getenv("API_USER")
    password = os.getenv("API_PW")

    assert username, "API_USER must be set in .env"
    assert password, "API_PW must be set in .env"

    # Instantiate client
    client = AirflowAPIClient("https://devairflow.alleghenycounty.us:8080")

    # Perform login
    client.login(username, password)
    assert client.jwt_token is not None, "Login failed, no JWT returned."

    # Perform API call
    pools = client.get_pools()
    assert pools is not None, "Pools API returned no data."

    # Print pools for debugging
    print("Retrieved pools:", pools)
