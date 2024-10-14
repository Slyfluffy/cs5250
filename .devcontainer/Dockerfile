FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get upgrade -y && \
    apt-get install python3 python3-boto3 curl unzip -y; \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
        -o "awscliv2.zip"; \
    unzip awscliv2.zip; \
    ./aws/install; \
    rm -rf aws awscliv2.zip