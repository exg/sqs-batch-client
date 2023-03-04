import itertools
import uuid
from unittest.mock import Mock

import boto3

from sqs_batch_client import SQSBatchClient


async def test_send(queue):
    client = boto3.client("sqs")
    batch_client = SQSBatchClient(client=client, max_attempts=4)
    invalid_entries = [
        {
            "Id": str(uuid.uuid4()),
            "MessageBody": "message",
            "MessageGroupId": "",
        }
        for i in range(20)
    ]
    entries = [
        {
            "Id": str(uuid.uuid4()),
            "MessageBody": f"message{i}",
            "MessageAttributes": {
                "attr1": {
                    "DataType": "String",
                    "StringValue": f"value{i}",
                },
                "attr2": {
                    "DataType": "Binary",
                    "BinaryValue": f"value{i}".encode(),
                },
            },
        }
        for i in range(20)
    ]

    response = await batch_client.send_messages(queue.url, itertools.chain(*zip(invalid_entries, entries)))
    assert len(response["Successful"]) == len(entries)
    assert len(response["Failed"]) == len(invalid_entries)

    messages = []
    while True:
        response = queue.receive_messages(MaxNumberOfMessages=10, MessageAttributeNames=["All"])
        if not response:
            break
        messages.extend(response)

    assert len(messages) == len(entries)
    for sent, received in zip(entries, messages):
        assert sent["MessageBody"] == received.body
        assert sent["MessageAttributes"] == received.message_attributes

    response = await batch_client.delete_messages(
        queue.url,
        (
            {
                "Id": str(i),
                "ReceiptHandle": message.receipt_handle,
            }
            for i, message in enumerate(messages)
        ),
    )
    assert len(response["Successful"]) == len(messages)


class SendMessageBatchMock:
    def __init__(self, error_count):
        self.error_count = error_count
        self.calls = 0

    # pylint: disable-next=invalid-name,unused-argument
    def send_message_batch(self, QueueUrl, Entries):
        self.calls += 1
        if self.calls < self.error_count:
            successful = Entries[::2]
            failed = Entries[1::2]
        else:
            successful = Entries
            failed = []
        return {
            "Successful": [
                {
                    "Id": entry["Id"],
                }
                for entry in successful
            ],
            "Failed": [
                {
                    "Id": entry["Id"],
                    "SenderFault": False,
                }
                for entry in failed
            ],
        }


async def test_send_should_retry_on_retryable_errors():
    max_attempts = 4
    client = Mock(send_message_batch=Mock())
    client.send_message_batch.side_effect = SendMessageBatchMock(max_attempts).send_message_batch
    batch_client = SQSBatchClient(client=client, max_attempts=max_attempts)
    entries = [
        {
            "Id": str(uuid.uuid4()),
            "MessageBody": f"message{i}",
        }
        for i in range(20)
    ]

    response = await batch_client.send_messages("test", entries)
    assert len(response["Successful"]) == len(entries)
    assert len(response["Failed"]) == 0
