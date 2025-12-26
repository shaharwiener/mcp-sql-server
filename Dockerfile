FROM python:3.11-bullseye

# Install system dependencies for ODBC
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean

WORKDIR /app

# Copy Requirements
COPY pyproject.toml .
# We might need to generate requirements.txt from toml or just install dependencies
# simplified for this context:
RUN pip install "mcp[cli]>=1.6.0" "structlog" "pydantic" "pyodbc>=5.1.0" "sqlglot>=19.0.0" "pyyaml" "python-dotenv"

COPY . .

# Default Config Path
ENV MCP_CONFIG_PATH=/app/config/config.yaml

CMD ["python", "server.py"]
