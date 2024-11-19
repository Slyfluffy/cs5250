from argparse import ArgumentParser
from boto3 import client, resource
from botocore.exceptions import ClientError
from json import dumps, loads
from logging import basicConfig, INFO
from time import sleep
from timeit import default_timer
from queue import Queue

from widget_app_base import WidgetAppBase

DEBUG_LEVEL = INFO

class WidgetConsumer(WidgetAppBase):
    def __init__(self) -> None:
        super().__init__()
        basicConfig(filename='log/consumer.log', level=DEBUG_LEVEL)
        self.logger.name = 'consumer_logger'
        # Only instantiate this if needed
        self.request_queue:Queue = None 
        self.receipt_handle_queue:Queue = None

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
                            default='widgets/',
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
        options. Returns true if all are valid, otherwise raises an error.
        '''
        if not self._verify_base_arguments(args):
            return False
        if args.max_runtime < 0:
            self.logger.error('max_runtime tried to be set as negative for some reason')
            raise ValueError('max_runtime cannot be negative!')
        if args.widget_bucket is None and \
           args.dynamodb_widget_table is None and \
           args.pdb_conn is None:
            self.logger.error('no widget save location was set before trying to use.')
            raise ValueError('widget-bucket, dynamodb-widget-table, or pdb-conn must be set in ' +
                'to use WidgetConsumer!')
        if args.request_bucket is not None and args.request_queue is not None:
            self.logger.error('Both a request bucket and request queue were specified!')
            raise Exception('Both a request bucket and request queue have been specified.' +
                             'Please only use one or the other.')
        if args.queue_wait_timeout < 0:
            self.logger.error('queue_wait_timeout tried to be set as negative for some reason')
            raise ValueError()
        if args.queue_visibility_timeout < 0:
            self.logger.error('queue_visibility_timeout tried to be set as negative ' +
                'for some reason')
            raise ValueError()
        
        return True

    def _create_service_clients(self) -> bool:
        '''Creates the services clients needed to run the consumer. This can be an S3 bucket or an
        SQS queue for gathering requests and an S3 bucket or dynamodb table for storage depending
        on arguments passed.
        '''
        if self.request_bucket is not None or self.widget_bucket is not None:
            self.aws_s3 = client('s3', region_name=self.region)
        if self.request_queue_url is not None:
            self.aws_sqs_queue = client('sqs', region_name=self.region)
            # Guess we need the queue after all!
            self.request_queue = Queue()
            self.receipt_handle_queue = Queue()

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
        '''Runner for Consumer. Consumes requests as they come in.'''
        self._create_service_clients()
        start_time = default_timer()

        # Assume we are not suppose to be running forever unless otherwise specified
        infinite_runtime:bool = True if self.max_runtime == 0 else False
        done:bool = False

        self.logger.info('Consumer ready. Waiting for requests...')
        while not done:
            try:
                request = self._get_request()
                if request['type'] != 'unknown':
                    self.logger.info('Received request of type %s: %s', request['type'], 
                                     request['requestId'])
                if self.request_bucket is not None:
                    self._delete_object_S3(self.request_bucket, request['request-bucket-key'])
                if self.process_request(request):
                    self.logger.info(f'{request['type']} request processed successfully')
                    if self.request_queue_url is not None:
                        self._delete_request_from_queue()
                else:
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
        '''Retrieves the request from either an S3 bucket or a queue. If both are setup, defaults
        to using the bucket and not the queue.
        '''
        if self.request_bucket is not None:
            return self._get_request_s3()
        if self.request_queue_url is not None:
            return self._get_request_queue()
        
        # Something wacky happened. Returning default dict
        self.logger.warning('Some unknown error happened when getting a request. ' + 
                            'Returning empty request dict.')
        return { 'type': 'unknown' } # Take advantage of our error handling above

    def _get_request_s3(self) -> dict:
        '''Retrieves a Widget request from S3'''
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
        '''Retrieves a request from the queue'''
        if not self.request_queue.empty(): # We are single-threaded for now, so this is ok.
            return self.request_queue.get()

        response:dict = self.aws_sqs_queue.receive_message(
            QueueUrl=self.request_queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=self.queue_visibility_timeout,
            WaitTimeSeconds=self.queue_wait_timeout
        )

        if 'Messages' not in response.keys():
            return { 'type': 'unknown' }

        for message in response['Messages']:
            self.request_queue.put(loads(message["Body"]))
            self.receipt_handle_queue.put(message['ReceiptHandle'])

        return self.request_queue.get()

    def _delete_request(self, request:dict) -> bool:
        '''Deletes the request from dynamodb or the queue depending on what is being used.'''
        if self.request_bucket is not None:
            return self._delete_object_S3(self.request_bucket, request['request-bucket-key'])
        if self.request_queue_url is not None:
            return self._delete_request_from_queue()
        
    def _delete_object_S3(self, bucket:str, key:str) -> bool:
        '''Actual implementation for any delete requests to an S3 bucket.'''
        try:
            self.logger.debug('Deleting Request: %s', key)
            self.aws_s3.delete_object(Bucket=bucket,Key=key)
            self.logger.debug('Request Deleted!')
        except Exception as e:
            self.logger.error(e)
            return False
        
        return True
    
    def _delete_request_from_queue(self) -> bool:
        try:
            receipt_handle:str = self.receipt_handle_queue.get()
            self.logger.debug('deleting message: %s', receipt_handle)
            self.aws_sqs_queue.delete_message(
                QueueUrl=self.request_queue_url,
                ReceiptHandle=receipt_handle
            )
        except ClientError as e:
            self.logger.error(e)
            return False
        
        return True

    def process_request(self, request:dict) -> bool:
        '''Processes any create, update, or delete requests. Raises a ValueError if a request is not
        one of those three.
        '''
        if request['type'] == 'create':
            self.logger.info('Request type is create. Creating widget...')
            return self.update_widget(request)
        if request['type'] == 'update':
            self.logger.info('Request type is update. Updating widget...')
            return self.update_widget(request)
        if request['type'] == 'delete':
            self.logger.info('Request type is delete. Deleting widget...')
            return self.delete_widget(request)
        else:
            raise ValueError('Cannot process request due to unknown request type: %s', 
                             request['type'])

    def update_widget(self, request:dict) -> bool:
        '''Creates or replaces a widget in S3 or dynamodb depending on passed args.'''
        if self.widget_bucket is not None:
            self.logger.info('Saving widget to S3')
            return self._update_widget_s3(request)
        if self.dynamodb_widget_table is not None:
            self.logger.info('Saving widget to DynamoDB')
            return self._update_widget_dynamodb(request)

    def _update_widget_s3(self, request:dict) -> bool:
        '''Base function to create/replace the widget in S3'''
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

    def _update_widget_dynamodb(self, request:dict) -> bool:
        '''Base function to create/replace the widget in dynamodb'''
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

    def delete_widget(self, request:dict) -> bool:
        '''Deletes the widget from S3 or Dynamodb according to the request and args passed.'''
        if self.widget_bucket is not None:
            self.logger.info('Deleting widget from S3')
            return self._delete_widget_s3(request)
        if self.dynamodb_widget_table is not None:
            self.logger.info('Deleting widget from DynamoDB')
            return self._delete_widget_dynamodb(request)

    def _delete_widget_s3(self, request:dict) -> bool:
        '''Deletes widgets from the S3 widget bucket'''
        key:str = self.widget_key_prefix
        if self.use_owner_in_prefix:
           key += (request['owner'] + '/')
        key += str(request['widgetId'])
        return self._delete_object_S3(self.widget_bucket, key)

    def _delete_widget_dynamodb(self, request:dict) -> bool:
        '''Base function that deletes the widget from the dynamodb table'''
        try:
            key:dict = { 'id': request['widgetId']}
            self.aws_dynamodb_table.delete_item(
                Key=key,
                ConditionExpression='attribute_exists(id)'
            )
        except Exception as e:
            self.logger.warning(e)
            return False
        
        return True

if __name__ == '__main__':
    app = WidgetConsumer()
    parser = app.get_consumer_parser()

    # Prep app with arguments
    args = parser.parse_args()
    app.verify_arguments(args)
    app.save_arguments(args)

    app.consume_requests()