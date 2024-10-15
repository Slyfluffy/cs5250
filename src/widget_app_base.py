'''Base class file for Widget apps. Contains shared data that is 
needed to work with AWS.
'''

class WidgetAppBase():
    '''Base class for all Widget Apps. Contains the needed data to work with AWS, as well as shared
    app defaults.
    '''
    # The following are the defaults (note implementations may
    # be different)
    profile:str = 'default'
    region:str = 'us-east-1'
    max_runtime:int = 0
    request_bucket:str = None
    use_owner_in_prefix: str = 'widgets/'
    request_queue_url:str = None
