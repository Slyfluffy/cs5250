from argparse import ArgumentParser
from widget_app_base import WidgetAppBase

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

    def __create_widget_s3(request:dict) -> bool:
        NotImplementedError()

    def __create_widget_dynamodb(request:dict) -> bool:
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