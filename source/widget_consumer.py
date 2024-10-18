from argparse import ArgumentParser
from boto3 import client
from botocore.exceptions import ClientError
from json import dumps

from source.widget_app_base import WidgetAppBase

class WidgetConsumer(WidgetAppBase):
    def __init__(self) -> None:
        super().__init__()
        self.logger.name = 'consumer_logger'

    def get_consumer_parser(self) -> ArgumentParser:
        '''Returns the parser for the consumer'''
        parser = self._get_basic_parser()
        
        # Many of the help prompts are based off of the `consumer.jar` example
        self.logger.debug('Adding Consumer App arguments to parser...')
        parser.add_argument('-mrt', '--max-runtime',
                            action='store',
                            type=int,
                            default=0,
                            help='Maximum runtime in milliseconds. 0 means no maximum ' + 
                                '(default: %(default)s)')
        parser.add_argument('-wb', '--widget-bucket',
                            action='store',
                            type=str,
                            default=None,
                            help='Name of S3 bucket holding widgets (default: %(default)s)')
        parser.add_argument('-wkp', '--widget-key-prefix',
                            action='store',
                            type=str,
                            default='\'widgets/\'',
                            help='Prefix for widget objects in S3 (default: %(default)s)')
        parser.add_argument('-dwt', '--dynamodb-widget-table',
                            action='store',
                            type=str,
                            default=None,
                            help='Name of DynamoDB table that holds widgets (default: %(default)s)')
        parser.add_argument('-pdbc', '--pdb-conn',
                            action='store',
                            type=str,
                            default=None,
                            help='Postgres Database connection string (default: %(default)s)')
        parser.add_argument('-pdbu', '--pdb-username',
                            action='store',
                            type=str,
                            default=None,
                            help='Postgres Database username (default: %(default)s)')
        parser.add_argument('-pdbp', '--pdb-password',
                            action='store',
                            type=str,
                            default=None,
                            help='Postgres Database password (default: %(default)s)')
        parser.add_argument('-qwt', '--queue-wait-time',
                            action='store',
                            type=int,
                            default=10,
                            help='The duration (in seconds) to wait for a message to arrive ' +
                                'in the request when tring to receive a message ' +
                                '(default: %(default)s)')
        parser.add_argument('-qvt', '--queue-visibility-timeout',
                            action='store',
                            type=int,
                            default=2,
                            help='The duration (in seconds) to the messages received from the ' + 
                                'queue are hidden from others (default: %(default)s)')
        self.logger.debug('Consumer argument options added! Returning parser.')
        
        return parser

    def verify_arguments(self, args: object) -> bool:
        '''Verifies that all arguments that can be validated are checked and verified to be good
        options. Returns true if all are valid, otherwise raises an error.'''
        if not self._verify_base_arguments(args):
            print("returned!")
            return False
        if args.max_runtime > 0:
            self.logger.error('max_runtime tried to be set as negative for some reason')
            raise ValueError('max_runtime cannot be negative!')
        if args.widget_bucket is None and \
           args.dynamodb_widget_table is None and \
           args.pdb_conn is None:
            self.logger.error('no widget save location was set before trying to use.')
            raise ValueError('widget-bucket, dynamodb-widget-table, or pdb-conn must be set in ' +
                'to use WidgetConsumer!')
        if args.queue_wait_timeout < 0:
            self.logger.error('queue_wait_timeout tried to be set as negative for some reason')
            raise ValueError()
        if args.queue_visibility_timeout < 0:
            self.logger.error('queue_visibility_timeout tried to be set as negative ' +
                'for some reason')
            raise ValueError()
        
        return True

    def _create_service_clients(self) -> bool:
        if self.widget_bucket is not None:
            self.aws_s3 = client('s3', region_name=self.region)

    def save_arguments(self, args: object) -> bool:
        '''Saves the arguments to WidgetConsumer to be used when running.'''
        self._save_base_arguments(args)
        
        self.logger.debug('Saving WidgetConsumer arguments...')
        self.widget_bucket:str = args.widget_bucket
        self.widget_key_prefix:str = args.widget_key_prefix
        self.dynamodb_widget_table:str = args.dynamodb_widget_table
        self.pdb_conn:str = args.pdb_conn
        self.pdb_username = args.pdb_username
        self.pdb_password = args.pdb_password
        self.queue_wait_timeout = args.queue_wait_timeout
        self.queue_visibility_timeout = args.queue_visibility_timeout
        self.logger.debug('WidgetConsumer arguments saved!')

        return True

    def get_requests_from_bucket(self):
        NotImplementedError()
        # time_running_ms:int = 0
        # DO
        #     TRY
        #         GET request from s3 bucket
        #         CONVERT request to json
        #         delete_request(request)
        #
        #         IF request['type'] == 'create'
        #             create_widget(request)
        #             continue
        #         ELSE IF request['type'] == 'update'
        #             update_widget(request)
        #             continue
        #         ELSE IF request['delete'] == 'delete'
        #             delete_widget(request)
        #             continue
        #         ELSE
        #             throw ERROR stating unknown request type
        # 
        #         time.sleep(100)
        #     EXCEPT ERROR about unknown request type
        #         log error and continue loop
        #     EXCEPT Ctrl+C 
        #         log user entered shutdown and shut down consumer
        #     EXCEPT anything else
        #         log error and shutdown
        # WHILE time_running_ms != 0 and time_running_ms < self.max_runtime

    def delete_request(request:dict) -> bool:
        NotImplementedError()

    def create_widget(request:dict) -> bool:
        NotImplementedError()

    def _create_widget_s3(self, request:dict) -> bool:
        key:str = self.widget_key_prefix
        if self.use_owner_in_prefix:
           key += (request['owner'] + '/')
        key += request['widgetId']
        try:
            self.aws_s3.put_object(Body=dumps(request), Bucket=self.widget_bucket, Key=key)
        except ClientError as e:
            self.logger.warning(e)
            return False
        
        return True

    def _create_widget_dynamodb(request:dict) -> bool:
        # TRY
        #     table.put_item(request)
        # EXCEPT Exception as e
        #     log.warning(e)
        #     return False
        #
        # return True
        NotImplementedError()

    def update_widget(request:dict) -> bool:
        NotImplementedError()

    def delete_Widget(request:dict) -> bool:
        NotImplementedError()


def verify_args():
    NotImplementedError()

if __name__ == '__main__':
    app = WidgetConsumer()
    parser = app.get_consumer_parser()
    parser.print_help()

    args = parser.parse_args()
    print(args)