from argparse import ArgumentParser
from boto3 import client, resource
from botocore.exceptions import ClientError
from json import dumps, loads
from logging import basicConfig, INFO

from time import sleep
from timeit import default_timer

from widget_app_base import WidgetAppBase

DEBUG_LEVEL = INFO

class WidgetConsumer(WidgetAppBase):
    def __init__(self) -> None:
        super().__init__()
        basicConfig(filename='../log/consumer.log', level=DEBUG_LEVEL)
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
        parser.add_argument('-qwt', '--queue-wait-timeout',
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
        if self.request_bucket is not None or self.widget_bucket is not None:
            self.aws_s3 = client('s3', region_name=self.region)
        if self.dynamodb_widget_table is not None:
            self.aws_dynamodb = resource('dynamodb', region_name=self.region)
            self.aws_dynamodb_table = self.aws_dynamodb.Table(self.dynamodb_widget_table)

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

    def consume_requests(self):
        self._create_service_clients()
        start_time = default_timer()

        # Assume we are not suppose to be running forever unless otherwise specified
        infinite_runtime:bool = True if self.max_runtime == 0 else False
        done:bool = False

        self.logger.info('Consumer ready. Waiting for requests...')
        while not done:
            try:
                request = self._get_request()
                self.logger.info('Received request: %s', request)
                if not self._delete_request(request):
                    continue # Error already logged from _delete_request, continue on
                if not self.process_request(request):
                    self.logger.info(f'{request['type']} request processed successfully')
                sleep(.01) # 100 milliseconds
            except ValueError:
                continue # Error already logged somewhere, continue on
            except KeyboardInterrupt:
                self.logger.info('\nCtrl+C detected. Shutting Down consumer...')
                return
            except Exception as e: # Some unknown error occured, do not continue running!
                self.logger.error(e)
                return
            
            # Consumer is only suppose to run until max_runtime is hit (unless infinite)
            current_runtime = (default_timer() - start_time) * 1000
            if not infinite_runtime and current_runtime < self.max_runtime:
                done = True

    def _get_request(self) -> dict:
        if self.request_bucket is not None:
            return self._get_request_s3()
        if self.request_queue_url is not None:
            return self._get_request_queue()
        
        # Something wacky happened. Returning default dict
        self.logger.warning('Some unknown error happened when getting a request. ' + 
                            'Returning empty request dict.')
        return { 'type': 'unknown' } # Take advantage of our error handling above

    def _get_request_s3(self) -> dict:
        try:
            # Get the first object key in the bucket since we don't know it
            response = self.aws_s3.list_objects_v2(Bucket=self.request_bucket, MaxKeys=1)
            key = response['Contents'][0]['Key']
            self.logger.debug('Found widget key: %s', key)
            
            # Now that we have the key, get the actual request and return it
            self.logger.debug('Getting object using key: %s', key)
            response = self.aws_s3.get_object(Bucket=self.request_bucket, Key=key)
            request = loads(response["Body"].read())
            request['request-bucket-key'] = key
            return request
        except KeyError:
            self.logger.warning('No requests found. Please wait until some more are complete')
        except Exception as e:
            self.logger.warning('Issue when getting request from s3: %s', e)
        
        return { 'type': 'unknown' }

    def _get_request_queue(self) -> dict:
        NotImplementedError()

    def _delete_request(self, request:dict) -> bool:
        try:
            self.logger.debug('Deleting Request: %s', request['request-bucket-key'])
            self.aws_s3.delete_object(Bucket=self.request_bucket,Key=request['request-bucket-key'])
            self.logger.debug('Request Deleted!')
        except Exception as e:
            self.logger.error(e)
            return False
        
        return True

    def process_request(self, request:dict) -> bool:
        if request['type'] == 'create':
            self.logger.info('Request type is create. Creating widget...')
            return self.create_widget(request)
        # if request['type'] == 'update':
        #     self.logger.info('Request type is update. Updating widget...')
        #     return self.update_widget(request)
        # if request['type'] == 'delete':
        #     self.logger.info('Request type is delete. Deleting widget...')
        #     return self.delete_widget(request)
        else:
            raise ValueError('Cannot process request due to unknown request type: %s', 
                             request['type'])

    def create_widget(self, request:dict) -> bool:
        if self.widget_bucket is not None:
            self.logger.info('Saving widget to S3')
            return self._create_widget_s3(request)
        if self.dynamodb_widget_table is not None:
            self.logger.info('Saving widget to DynamoDB')
            return self._create_widget_dynamodb(request)

    def _create_widget_s3(self, request:dict) -> bool:
        key:str = self.widget_key_prefix
        if self.use_owner_in_prefix:
           key += (request['owner'] + '/')
        key += str(request['widgetId'])
        try:
            self.logger.debug('Placing object into s3 using key: %s', key)
            self.aws_s3.put_object(Body=dumps(request), Bucket=self.widget_bucket, Key=key)
            self.logger.debug('Saved!')
        except ClientError as e:
            self.logger.warning(e)
            return False
        
        return True

    def _create_widget_dynamodb(self, request:dict) -> bool:
        try:
            request.pop('requestId') # don't need this one either
            # Adjust the id before sending it to dynamodb
            request['id'] = request.pop('widgetId')

            # Then go and save it
            self.aws_dynamodb_table.put_item(Item=request)
        except Exception as e:
            self.logger.warning(e)
            return False
        
        return True

    def update_widget(request:dict) -> bool:
        NotImplementedError()

    def delete_Widget(request:dict) -> bool:
        NotImplementedError()

if __name__ == '__main__':
    app = WidgetConsumer()
    parser = app.get_consumer_parser()

    # Prep app with arguments
    args = parser.parse_args()
    app.verify_arguments(args)
    app.save_arguments(args)

    app.consume_requests()