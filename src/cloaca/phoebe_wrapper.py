import os
from phoebe_bird import AsyncPhoebe

from dotenv import load_dotenv

load_dotenv()

phoebe_client = AsyncPhoebe(
    api_key=os.environ.get("EBIRD_API_KEY"),
)


def get_phoebe_client():
    return phoebe_client
