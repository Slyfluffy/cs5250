FROM public.ecr.aws/lambda/python:3.12 AS consumer
WORKDIR /handler
COPY source/widget_app_base.py source/widget_request_handler.py /handler/
RUN pip install --no-cache-dir boto3;
ENTRYPOINT [ "python3", "widget_request_handler.py" ]