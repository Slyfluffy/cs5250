from boto3 import client
from botocore.exceptions import ClientError
from logging import error


REGION = 'us-east-1'

def get_buckets_list() -> object:
    try:
        s3_client = client('s3', region_name=REGION)
        return s3_client.list_buckets()
    except ClientError as e:
        error(e)
        return False
    
def print_buckets() -> None:
    buckets_dict = get_buckets_list()

    print("Buckets available:")
    for bucket in buckets_dict['Buckets']:
        print(f'\tbucket: {bucket["Name"]}')

print_buckets()