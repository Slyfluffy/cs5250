'''Empty as there are no functions (yet) in widget_app_base.py'''
from pytest import raises

from source.widget_app_base import WidgetAppBase

class BaseArgReplica:
    def __init__(self) -> None:
        self.profile:str = 'default'
        self.region:str = 'us-east-1'
        self.request_bucket:str = None
        self.use_owner_in_prefix:bool = False
        self.request_queue:str = None

class TestWidgetAppBaseVerifyBaseArguments:
    def test_verify_base_arguments_request_bucket(self):
        # setup
        args = BaseArgReplica()
        args.request_bucket = 'test'
        app = WidgetAppBase()
        
        # exercise and verify
        assert app._verify_base_arguments(args)

    def test_verify_base_arguments_request_queue(self):
        # setup
        args = BaseArgReplica()
        args.request_queue = 'test'
        app = WidgetAppBase()
        
        # exercise and verify
        assert app._verify_base_arguments(args)

    def test_verify_base_arguments_no_request_bucket_no_request_queue(self):
        # setup
        args = BaseArgReplica()
        app = WidgetAppBase()
        
        # exercise and verify
        with raises(ValueError):
            app._verify_base_arguments(args)
