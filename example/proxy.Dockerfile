FROM python:3.12-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy the entire application code
COPY . .

# Install dependencies
RUN uv pip install --system --no-cache -e .["all"]

EXPOSE 3306
CMD ["python", "-m", "mimir.sql.proxy"]
