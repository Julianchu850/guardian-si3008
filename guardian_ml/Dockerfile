# Se construye desde la raíz del repositorio.
FROM python:3.12-slim
WORKDIR /app
COPY guardian_ml/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY guardian_ml/guardian_ml ./guardian_ml
ENTRYPOINT ["python", "-m", "guardian_ml.scorer"]
