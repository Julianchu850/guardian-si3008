# Se construye desde la raíz del repositorio (incluye el guardián para la política "guarded").
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir numpy pandas confluent-kafka scikit-learn==1.9.1 joblib boto3
COPY simulator/guardian_sim ./guardian_sim
COPY guardian_ml/guardian_ml ./guardian_ml
ENTRYPOINT ["python", "-m", "guardian_sim"]
