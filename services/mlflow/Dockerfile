FROM python:3.12.7-slim

RUN pip install mlflow

EXPOSE 5000

# Command to start the MLflow server
CMD ["mlflow", "server", "--backend-store-uri", "sqlite:///mlflow.db", "--default-artifact-root", "/mlflow/artifacts", "--host", "0.0.0.0"]
