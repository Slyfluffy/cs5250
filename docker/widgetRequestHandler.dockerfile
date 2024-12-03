FROM public.ecr.aws/lambda/python:3.12 AS consumer
RUN pip install boto3;
COPY source/widget_request_handler.py ${LAMBDA_TASK_ROOT}
CMD [ "widget_request_handler.handler" ]