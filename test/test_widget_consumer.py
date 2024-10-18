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

class TestWidgetConsumer:
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
