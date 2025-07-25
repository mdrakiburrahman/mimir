FROM python:3.12.10
ENV PYTHONUNBUFFERED=True
ENV APP_HOME=/root
ENV PYTHONPATH=$APP_HOME
ENV CONFIGS_PATH=$APP_HOME/configs
ENV SECRETS_PATH=$APP_HOME/secrets

WORKDIR $APP_HOME

# Install uv
RUN pip install uv

# Copy the entire application code
COPY . $APP_HOME

# Install dependencies
RUN uv pip install --system --no-cache -e .["all"]

COPY ./example/configs $APP_HOME/configs
COPY ./example/secrets $APP_HOME/secrets

# Copy specific test fixtures needed for E2E tests
COPY ./tests/fixtures/configs/sources/duckdb_source.yaml $APP_HOME/configs/sources/
COPY ./tests/fixtures/configs/metrics/stock_level.yaml $APP_HOME/configs/metrics/
COPY ./tests/fixtures/secrets/test_duckdb.json $APP_HOME/secrets/
COPY ./tests/fixtures/data/inventory.csv $APP_HOME/data/

CMD ["uvicorn", "example.server.main:app", "--host", "0.0.0.0", "--port", "8080"]
