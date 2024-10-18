from boto3 import client
from moto import mock_aws
from pytest import raises

from source.widget_consumer import WidgetConsumer
from test_widget_app_base import BaseArgReplica

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
        response = app.aws_s3.get_object(Bucket=args.widget_bucket, Key='widgets/tester/1')
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
    def test():
        NotImplementedError()

@mock_aws
class TestWidgetConsumerCreateWidget:
    def test():
        NotImplementedError()

@mock_aws
class TestWidgetConsumerGetRequestsFromBucket:
    def test():
        NotImplementedError()