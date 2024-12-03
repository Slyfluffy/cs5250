
from boto3 import client
from botocore.exceptions import ClientError
from json import dumps, loads
from logging import basicConfig, getLogger, INFO
from os import environ
from uuid import uuid4

if getLogger().hasHandlers():
    getLogger().setLevel(INFO)
else:
    basicConfig(level=INFO)

logger = getLogger()

def handler(event, context) -> None:
    '''AWS lambda function'''
    logger.info('Sending new message to SQS...')
    logger.debug('REGION: %s', environ['REGION'])
    logger.debug('QUEUE_URL: %s', environ['QUEUE_URL'])

    request = loads(event['body'])
    request['requestId'] = context.aws_request_id
    logger.info('requestId: %s', request['requestId'])

    sqs = client('sqs', region_name=environ['REGION'])
    result = handle_request(request, sqs)

    logger.debug('Result of handling request: %s', result.__str__())
    logger.info('Request %s sent to SQS!', request['requestId'])

def handle_request(request:dict, sqs) -> bool:
    '''Handles the widget requests sent from devices.'''
    if request['type'] not in { 'create', 'update', 'delete' }:
        logger.error('%s is not a valid request type', request['type'])
        return False
    if request['type'] == 'create':
        widgetId = str(uuid4())
        logger.info('widgetId created: %s', widgetId)
        request['widgetId'] = widgetId
    
    # Send the request to the queue
    try:
        response = sqs.send_message(
            QueueUrl=environ['QUEUE_URL'],
            MessageBody=dumps(request)
        )
        logger.info('Sent message %s', response['MessageId'])
    except ClientError as e:
        logger.error('Error occurred talking to AWS: %s', exc_info=e)
        return False
    except KeyError:
        logger.error('QUEUE_URL environment variable was not set!')
        return False

    return True