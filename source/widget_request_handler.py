
from boto3 import client
from botocore.exceptions import ClientError
from os import environ
from uuid import uuid4

def widget_request_handler(event: dict, context) -> None:
    NotImplementedError()
    # request = json.loads(event['Body'])
    # request['requestId'] = event['requestId']
    # if request['type'] not in { 'create', 'update', 'delete' }:
    #     LOG error
    #     return
    # if request['type'] == 'create':
    #     widgetId = str(uuid4())
    #     LOG widgetID
    #     request['widgetId'] = widgetId
    #
    # # Send the request to the queue
    # try:
    #     sqs = client('sqs', region_name=self.region)
    #     response = sqs.send_message(
    #         QueueUrl=environ['QUEUE_URL'],
    #         MessageBody=json.dumps(request)
    #     )
    #    LOG widgetId, requestId, and messageId
    # except ClientError as e:
    #     LOG error
    # except KeyError as e:
    #     Log error