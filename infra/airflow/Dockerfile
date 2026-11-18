# Imagen oficial de Airflow (ya trae el proveedor de Amazon) + simulador y entrenamiento.
# scikit-learn queda fijo en la versión de las restricciones de Airflow, la misma que
# usa el guardián para cargar el modelo.
FROM apache/airflow:3.3.2-python3.12
RUN pip install --no-cache-dir "apache-airflow==3.3.2" numpy pandas pyarrow joblib "scikit-learn==1.9.1" \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
