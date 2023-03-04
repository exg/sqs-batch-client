import os

import boto3
import moto
import pytest

os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"


@pytest.fixture(scope="session", autouse=True)
def aws_mock():
    with moto.mock_sqs():
        yield


@pytest.fixture(name="queue")
def queue_fixture():
    sqs = boto3.resource("sqs")
    queue = sqs.create_queue(QueueName="test-queue")
    yield queue
    queue.delete()
