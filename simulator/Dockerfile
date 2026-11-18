FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir numpy pandas confluent-kafka
COPY guardian_sim ./guardian_sim
ENTRYPOINT ["python", "-m", "guardian_sim"]
