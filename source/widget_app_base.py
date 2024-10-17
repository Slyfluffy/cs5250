'''Base class file for Widget apps. Contains shared data that is 
needed to work with AWS.
'''

from argparse import ArgumentParser
from logging import Logger

class WidgetAppBase():
    '''Base class for all Widget Apps. Contains the needed data to work with AWS, as well as shared
    app defaults.
    '''
    def __init__(self) -> None:
        self.logger = Logger('WidgetAppBase')

    def _verify_base_arguments(self, args: object) -> bool:
        '''Verifies that the base needed arguments are met. If they are not, throws an error.
        Returns true if all the arguments are valid.
        '''

        if args.request_bucket is None and args.request_queue is None:
            raise ValueError()

    def _save_base_arguments(self, args: object) -> None:
        '''Saves the passed arguments into the class.'''
        self.logger.debug('Saving WidgetAppBase arguments...')
        self.profile:str = args.profile
        self.region:str = args.region
        self.max_runtime:int = args.max_runtime
        self.request_bucket:str = args.request_bucket
        self.use_owner_in_prefix: str = args.use_owner_in_prefix
        self.request_queue_url:str = args.request_queue
        self.logger.debug('WidgetAppBase arguments saved!')

    def _get_basic_parser(self) -> ArgumentParser:
        '''Returns a ArgumentParser filled with the basic
        arguments needed for a Widget Application.
        '''
        parser = ArgumentParser()
        
        # Many of the help prompts are based off of the `producer/consumer.jar` examples
        self.logger.debug('Adding basic WidgetApp arguments to parser...')
        parser.add_argument('-p', '--profile',
                            action='store',
                            type=str,
                            default='default',
                            help='AWS profile to use for credentials (default: %(default)s)')
        parser.add_argument('-r', '--region',
                            action='store',
                            type=str,
                            default='us-east-1',
                            help='Name of AWS region to use (default: %(default)s)')
        parser.add_argument('-rb', '--request-bucket',
                            action='store',
                            type=str,
                            default=None,
                            help='Name of bucket containing widget requests (default: %(default)s)')
        parser.add_argument('-uop', '--use-owner-in-prefix',
                            action='store_true',
                            default=False,
                            help="Use the owner's name in the S3 object's prefix " +
                                "(default: %(default)s)")
        parser.add_argument('-rq', '--request-queue',
                            action='store',
                            type=str,
                            default=None,
                            help='URL of queue containing widget requests (default: %(default)s)')
        self.logger.debug('WidgetApp argument options added! Returning parser.')

        return parser