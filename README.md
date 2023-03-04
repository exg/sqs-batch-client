# SQS Batch Client

SQSBatchClient is a class that provides an async interface to send and
delete multiple messages to and from an SQS queue by delegating to the
`send_message_batch` and `delete_message-batch` methods of [Boto3 SQS
Client](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html),
respectively.


## Usage

First, create an SQS Batch Client:
```python
import boto3
from sqs_batch_client import SQSBatchClient
client = boto3.client("sqs")
batch_client = SQSBatchClient(client=client, max_attempts=4)
```
Then, send or delete some messages:

### Send messages

```python
import asyncio

entries = [
    {
        "Id": "1",
        "MessageBody": "message1",
        "MessageAttributes": {
            "type": {
                "DataType": "String",
                "StringValue": "type1",
            }
        },
    },
    {
        "Id": "2",
        "MessageBody": "message2",
        "MessageAttributes": {
            "type": {
                "DataType": "String",
                "StringValue": "type2",
            }
        },
    },
]

loop = asyncio.get_event_loop()
coro = await batch_client.send_messages("queue", entries)
response = loop.run_until_complete(coro)
```
`send_messages` returns the successful and failed responses in the same
format as that returned by the `send_message_batch` method of Boto3 SQS
client.

### Delete messages

```python
import asyncio

entries = [
    {
        "Id": "1",
        "ReceiptHandle": "handle1",
    },
    {
        "Id": "2",
        "ReceiptHandle": "handl2",
    },
]

loop = asyncio.get_event_loop()
coro = await batch_client.delete_messages("queue", entries)
response = loop.run_until_complete(coro)
```
`delete_messages` returns the successful and failed responses in the same
format as that returned by the `delete_message_batch` method of Boto3 SQS
client.
