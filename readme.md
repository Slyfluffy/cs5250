# Readme

## Setup

Run the folder in a devcontainer, then install the pip packages below.

## pip packages

* boto3
* pytest
* pytest-mock
* moto[all]

## Running the unit tests

To run the unit tests, use `python3 -m pytest` at the root of the repo.

## Running the consumer

To run the consumer using the S3 request bucket and S3 web bucket, do the following from the `source` folder:

`python3 widget_consumer.py -rb {request-bucket} -wb {widget-bucket}`

For DynamoDb, do the following:

`python3 widget_consumer.py -rb {request-bucket} -dwt {dynamodb-table-name}`

## Resource links

* [python argparse](https://docs.python.org/3/library/argparse.html)
* [pytest](https://docs.pytest.org/en/stable/index.html#)
* [python json](https://docs.python.org/3/library/json.html)
* [python logging](https://docs.python.org/3/library/logging.html)
* [aws boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
* [pytest and aws](https://pytest-with-eric.com/mocking/pytest-mocking/#Mock-A-Class)
