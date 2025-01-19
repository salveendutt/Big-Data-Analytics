FROM python:3.12.7-slim

# Set working directory
WORKDIR /app

# Install necessary dependencies
RUN apt-get update && apt-get install -y cron && \
    pip install mlflow pyspark

# Expose the port for the MLflow server
EXPOSE 5000

# Copy your services and scripts
COPY ./services/mlflow /app
# COPY ./training/model_dataset1.py /training/model_dataset1.py

# Create the cron job that runs the script every minute
# RUN echo "* * * * * echo 'Hello workld! KDBG' >> /var/log/cron.log 2>&1" > /etc/cron.d/model-cron

# Give execution rights to the cron job
# RUN chmod 0644 /etc/cron.d/model-cron

COPY ./services/mlflow/crontab /etc/cron.d/crontab
# COPY ./crontab /etc/cron.d/crontab 
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

# # Apply the cron job to the cron service
# RUN crontab /etc/cron.d/model-cron

# Create a log file for cron
# RUN touch /var/log/cron.log
RUN touch /var/log/cron.log && chmod 666 /var/log/cron.log

# Create a start.sh script to start both cron and mlflow server
# COPY ./start.sh /start.sh
RUN chmod +x ./start.sh

# Default command to run the start script
CMD ["./start.sh"]
