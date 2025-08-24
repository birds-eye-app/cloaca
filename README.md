# cloaca

## Development

Run the application:
```bash
uv run fastapi dev src/cloaca/main.py
```

## Testing

Run all tests:
```bash
python -m pytest
```

Run tests for a specific module:
```bash
python -m pytest tests/api/ -v
python -m pytest tests/parsing/ -v
python -m pytest tests/scripts/ -v
```

Run tests with verbose output:
```bash
python -m pytest -v
```

Note: Some tests in `tests/db/` may have import issues and might need to be run individually or skipped.
