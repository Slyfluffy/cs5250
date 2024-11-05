FROM python:3.13 AS pytest
WORKDIR /workspace
RUN pip install --no-cache-dir \
    boto3 \
    moto[all] \
    pytest \
    pytest-mock
ENTRYPOINT [ "python3", "-m", "pytest" ]