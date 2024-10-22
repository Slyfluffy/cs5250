from boto3 import client
from json import dumps
from moto import mock_aws
from pytest import raises

from source.widget_consumer import WidgetConsumer
from test.test_widget_app_base import BaseArgReplica

class ConsumerArgReplica(BaseArgReplica):
    def __init__(self) -> None:
        super().__init__()
        self.max_runtime:int = 0
        self.widget_bucket:str = None
        self.widget_key_prefix:str = 'widgets/'
        self.dynamodb_widget_table:str = None
        self.pdb_conn:str = None
        self.pdb_username:str = None
        self.pdb_password:str = None
        self.queue_wait_timeout:int = 10
        self.queue_visibility_timeout:int = 2

class TestWidgetConsumerVerifyArguments:
    def test_verify_arguments_negative_max_runtime(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.max_runtime = -1
        app = WidgetConsumer()
        
        # exercise and verify
        with raises(ValueError):
            app.verify_arguments(args)

    def test_verify_arguments_no_output_database(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        app = WidgetConsumer()
        
        # exercise and verify
        with raises(ValueError):
            app.verify_arguments(args)

    def test_verify_arguments_s3_bucket_database(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.widget_bucket = 'test'
        app = WidgetConsumer()
        
        # exercise and verify
        assert app.verify_arguments(args)

    def test_verify_arguments_dynamodb_database(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.dynamodb_widget_table = 'test'
        app = WidgetConsumer()
        
        # exercise and verify
        assert app.verify_arguments(args)

    def test_verify_arguments_postgress_database(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.pdb_conn = 'test'
        app = WidgetConsumer()
        
        # exercise and verify
        assert app.verify_arguments(args)

    def test_verify_arguments_negative_queue_wait_timeout(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.dynamodb_widget_table = 'test'
        args.queue_wait_timeout = -1
        app = WidgetConsumer()
        
        # exercise and verify
        with raises(ValueError):
            app.verify_arguments(args)

    def test_verify_arguments_negative_queue_visibility_timeout(self):
        # setup
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.dynamodb_widget_table = 'test'
        args.queue_visibility_timeout = -1
        app = WidgetConsumer()
        
        # exercise and verify
        with raises(ValueError):
            app.verify_arguments(args)

@mock_aws
class TestWidgetConsumerGetRequestS3:
    def test_valid_get_request(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1',
            'request-bucket-key': '1',
            'type': 'create'
        }

        ## mock s3
        ### setup test object for request_bucket
        app.aws_s3.create_bucket(Bucket=args.request_bucket)
        app.aws_s3.put_object(Body=dumps(request),
                               Bucket=app.request_bucket, 
                               Key=request['request-bucket-key'])

        # exercise
        test_request = app._get_request_s3()

        # verify
        assert request == test_request

    def test_errored_get_request(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## mock s3
        ### setup test object for request_bucket
        app.aws_s3.create_bucket(Bucket=args.request_bucket)
        # exercise
        test_request = app._get_request_s3()

        # verify
        assert { 'type' : 'unknown' } == test_request

# @mock_aws
# class TestWidgetConsumerGetRequestsQueue:
#     def test_valid_get_request(self):
#         NotImplementedError()

#     def test_errored_get_request(self):
#         NotImplementedError()

@mock_aws
class TestWidgetConsumerDeleteRequest:
    def test_valid_delete_request(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1',
            'request-bucket-key': '1'
        }
        
        ## mock s3
        app.aws_s3.create_bucket(Bucket=args.request_bucket)

        ## Prep bucket to delete
        app.aws_s3.put_object(Body=dumps(request), 
                              Bucket=args.request_bucket, 
                              Key=request['request-bucket-key'])

        # exercise and verify
        assert app._delete_request(request)

    def test_invalid_delete_request(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1',
            'request-bucket-key': '2'
        }
        
        ## mock s3
        app.aws_s3.create_bucket(Bucket=args.request_bucket)

        ## Prep bucket to delete
        app.aws_s3.put_object(Body=dumps(request), 
                              Bucket=args.request_bucket, 
                              Key=request['request-bucket-key'])

        # exercise and verify
        assert app._delete_request(request)

class TestWidgetConsumerProcessRequest:
    def test_valid_create_widget_request(self, mocker):
        # setup
        ## request
        request:dict[str, str] = {
            'type': 'create',
            'owner': 'tester',
            'widgetId': '1',
        }

        # app (mock the create_widget function so we don't do all the work)
        app = WidgetConsumer()
        mocker.patch.object(app, 'create_widget', return_value=True)
        
        # Exercise and Verify
        assert app.process_request(request)

    # def test_valid_update_widget_request():
    #     NotImplementedError()

    # def test_valid_delete_widget_request():
    #     NotImplementedError()

    def test_invalid_request(self):
        # setup
        ## request
        request:dict[str, str] = {
            'type': 'unknown',
            'owner': 'tester',
            'widgetId': '1',
        }

        # app (mock the create_widget function so we don't do all the work)
        app = WidgetConsumer()
        
        # Exercise and Verify
        with raises(ValueError):
            app.process_request(request)

@mock_aws
class TestWidgetConsumerCreateWidgetS3:
    def test_create_widget_s3_mocked_no_name_in_prefix(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.widget_bucket = 'test-bucket'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1'
        }

        ## mock s3
        app.aws_s3.create_bucket(Bucket=args.widget_bucket)

        # exercise and verify
        assert app._create_widget_s3(request)

    def test_create_widget_s3_mocked_name_in_prefix(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.widget_bucket = 'test-bucket'
        args.use_owner_in_prefix = True

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1'
        }

        ## mock s3
        app.aws_s3.create_bucket(Bucket=args.widget_bucket)

        # exercise and verify
        assert app._create_widget_s3(request)

        # exercise and verify
        app.aws_s3.get_object(Bucket=args.widget_bucket, Key='widgets/tester/1')
        assert True # We got a response so we are good

    def test_create_widget_s3_mocked_bad_request(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.widget_bucket = 'test-bucket'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_s3 = client('s3', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1'
        }

        ## mock s3
        app.aws_s3.create_bucket(Bucket='test')

        # exercise and verify
        assert not app._create_widget_s3(request)

@mock_aws
class TestWidgetConsumerCreateWidgetDynamoDB:
    def test_valid_create_widget_dynamodb(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.dynamodb_widget_table = 'test-table'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_dynamodb = client('dynamodb', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'requestId': '1',
            'widgetId': '1'
        }

        ## mock dynamodb
        app.aws_dynamodb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                },
            ],
            TableName=args.dynamodb_widget_table,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
        )

        # exercise and verify
        assert app._create_widget_dynamodb(request)

    def test_invalid_create_widget_dynamodb(self):
        # setup
        ## args
        args = ConsumerArgReplica()
        args.request_bucket = 'test'
        args.dynamodb_widget_table = 'test-table'

        ## app
        app = WidgetConsumer()
        app.save_arguments(args)
        app._create_service_clients()
        app.aws_dynamodb = client('dynamodb', region_name='us-east-1')

        ## request
        request:dict[str, str] = {
            'owner': 'tester',
            'widgetId': '1'
        }

        ## mock dynamodb
        app.aws_dynamodb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'widgetId',
                    'AttributeType': 'S'
                },
            ],
            TableName='test',
            KeySchema=[
                {
                    'AttributeName': 'widgetId',
                    'KeyType': 'HASH'
                },
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
        )

        # exercise and verify
        assert not app._create_widget_dynamodb(request)