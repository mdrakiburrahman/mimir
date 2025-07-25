#!/bin/bash
set -e

# Wait for the data to be downloaded
while [ ! -f /pagila_data/pagila-master/pagila-schema.sql ]; do
  echo "Waiting for Pagila data to be downloaded..."
  sleep 2
done

# Run the schema and data scripts
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /pagila_data/pagila-master/pagila-schema.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f /pagila_data/pagila-master/pagila-data.sql
