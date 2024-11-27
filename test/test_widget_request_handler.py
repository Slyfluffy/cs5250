from boto3 import client
from json import dumps
from moto import mock_aws
from os import environ

from source.widget_request_handler import handle_request

class TestWidgetRequestHandler():
    @mock_aws
    def test_valid_widget_request_create(self):
        # setup
        request = {
            'requestId': '1',
            'type': 'create',
            'owner': 'tester'
        }

        ## set environment variables
        environ['REGION'] = 'us-east-1'

        ## setup client
        sqs = client('sqs', region_name='us-east-1')
        environ['QUEUE_URL'] = sqs.create_queue(QueueName='test-queue')['QueueUrl']

        # Exercise
        assert handle_request(request, sqs)

        # verify
        response:dict = sqs.receive_message(
            QueueUrl=environ['QUEUE_URL'],
            MaxNumberOfMessages=1
        )
        assert response['Messages'][0]["Body"]

    @mock_aws
    def test_valid_widget_request_update(self):
        # setup
        request = {
            'requestId': '1',
            'type': 'create',
            'owner': 'tester'
        }

        ## set environment variables
        environ['REGION'] = 'us-east-1'

        ## setup client
        sqs = client('sqs', region_name='us-east-1')
        environ['QUEUE_URL'] = sqs.create_queue(QueueName='test-queue')['QueueUrl']

        # Exercise
        assert handle_request(request, sqs)

        # verify
        response:dict = sqs.receive_message(
            QueueUrl=environ['QUEUE_URL'],
            MaxNumberOfMessages=1
        )
        assert response['Messages'][0]["Body"]
    
    @mock_aws
    def test_valid_widget_request_delete(self):
        # setup
        request = {
            'requestId': '1',
            'type': 'create',
            'owner': 'tester'
        }

        ## set environment variables
        environ['REGION'] = 'us-east-1'

        ## setup client
        sqs = client('sqs', region_name='us-east-1')
        environ['QUEUE_URL'] = sqs.create_queue(QueueName='test-queue')['QueueUrl']

        # Exercise
        assert handle_request(request, sqs)

        # verify
        response:dict = sqs.receive_message(
            QueueUrl=environ['QUEUE_URL'],
            MaxNumberOfMessages=1
        )
        assert response['Messages'][0]["Body"]

    @mock_aws
    def test_invalid_request_type(self):
        # setup
        request = {
            'requestId': '1',
            'type': 'unknown',
            'owner': 'tester'
        }

        ## set environment variables
        environ['REGION'] = 'us-east-1'
        environ['QUEUE_URL'] = 'test.test'

        ## setup client
        sqs = client('sqs', region_name='us-east-1')

        # Exercise
        assert not handle_request(request, sqs)

    @mock_aws
    def test_widget_request_no_queue_url(self):
        # setup
        request = {
            'requestId': '1',
            'type': 'create',
            'owner': 'tester'
        }

        ## set environment variables
        environ['REGION'] = 'us-east-1'

        ## setup client
        sqs = client('sqs', region_name='us-east-1')
        sqs.create_queue(QueueName='test-queue')['QueueUrl']

        # Exercise
        assert not handle_request(request, sqs)