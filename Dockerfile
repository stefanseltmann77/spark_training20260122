FROM python:3.12-slim-bullseye
RUN apt update
RUN apt install curl mlocate default-jdk -y
RUN pip install --no-cache-dir pandas matplotlib confluent-kafka pyarrow pyspark>=4.0.0 pytest delta-spark
RUN mkdir ~/spark_training/
WORKDIR /root/spark_training
