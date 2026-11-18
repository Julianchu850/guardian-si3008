FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bronze_sink ./bronze_sink
ENTRYPOINT ["python", "-m", "bronze_sink"]
