FROM python:3.10-slim

WORKDIR /app

# Install system dependencies needed for psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
COPY dashboard/streamlit/requirements.txt dashboard_requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r dashboard_requirements.txt

# Copy everything else
COPY . .

# Expose port
EXPOSE 8501

# Command to run the application
CMD ["streamlit", "run", "dashboard/streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
