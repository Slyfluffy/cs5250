FROM python:3.13 AS consumer
WORKDIR /consumer
COPY source/widget_app_base.py source/widget_consumer.py /consumer/
RUN pip install --no-cache-dir boto3;
ENTRYPOINT [ "python3", "widget_consumer.py" ]