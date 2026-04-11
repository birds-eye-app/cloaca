import os

# Set a dummy eBird API key so the phoebe client can be imported.
# All actual API calls are mocked in tests.
os.environ.setdefault("EBIRD_API_KEY", "test-dummy-key")
