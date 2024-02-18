from __future__ import annotations

import asyncio
import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from itertools import count
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient
    from mypy_boto3_sqs.type_defs import (
        BatchResultErrorEntryTypeDef,
        DeleteMessageBatchRequestEntryTypeDef,
        DeleteMessageBatchResultEntryTypeDef,
        SendMessageBatchRequestEntryTypeDef,
        SendMessageBatchResultEntryTypeDef,
    )


MAX_MESSAGE_SIZE = 262144


def _full_jitter_backoff(cap: float, base: float) -> Iterator[float]:
    for attempt in count():
        yield random.uniform(0, min(cap, base * 2**attempt))


def _get_batch_len(sizes: Iterable[int], max_batch_len: int, max_batch_size: int) -> int:
    batch_len = 0
    batch_size = 0
    for size in sizes:
        if batch_len < max_batch_len and batch_size + size <= max_batch_size:
            batch_len += 1
            batch_size += size
        else:
            break
    return batch_len


@dataclass
class _EntryEnvelope:
    entry: SendMessageBatchRequestEntryTypeDef
    size: int = field(init=False)

    def __post_init__(self) -> None:
        self.size = len(self.entry["MessageBody"].encode()) + sum(
            len(k) + len(v["DataType"]) + len(v.get("BinaryValue", b"")) + len(v.get("StringValue", "").encode())
            for k, v in self.entry.get("MessageAttributes", {}).items()
        )


class SQSBatchClientSendResults(TypedDict):
    Successful: list[SendMessageBatchResultEntryTypeDef]
    Failed: list[BatchResultErrorEntryTypeDef]


class SQSBatchClientDeleteResults(TypedDict):
    Successful: list[DeleteMessageBatchResultEntryTypeDef]
    Failed: list[BatchResultErrorEntryTypeDef]


class SQSBatchClient:
    def __init__(self, client: SQSClient, max_attempts: int):
        self.client = client
        self.max_attempts = max_attempts

    async def send_messages(
        self,
        queue_url: str,
        entries: Iterable[SendMessageBatchRequestEntryTypeDef],
    ) -> SQSBatchClientSendResults:
        envelopes = (_EntryEnvelope(entry) for entry in entries)
        pending = tuple(envelope for envelope in envelopes if envelope.size <= MAX_MESSAGE_SIZE)
        results: SQSBatchClientSendResults = {
            "Successful": [],
            "Failed": [],
        }

        delay_gen = _full_jitter_backoff(20, 2)
        attempts = 0
        while pending:
            batch_len = _get_batch_len((envelope.size for envelope in pending), 10, MAX_MESSAGE_SIZE)
            batch = [envelope.entry for envelope in pending[:batch_len]]
            response = await asyncio.to_thread(
                self.client.send_message_batch,
                QueueUrl=queue_url,
                Entries=batch,
            )

            results["Successful"].extend(response.get("Successful", []))
            results["Failed"].extend(result for result in response.get("Failed", []) if result["SenderFault"])
            errors = {result["Id"]: result for result in response.get("Failed", []) if not result["SenderFault"]}

            if errors:
                attempts += 1
                if attempts < self.max_attempts:
                    delay = next(delay_gen)
                    await asyncio.sleep(delay)
                else:
                    results["Failed"].extend(errors.values())
                    break

            retryable = tuple(envelope for envelope in pending[: len(batch)] if envelope.entry["Id"] in errors)
            pending = retryable + pending[len(batch) :]

        return results

    async def delete_messages(
        self,
        queue_url: str,
        entries: Iterable[DeleteMessageBatchRequestEntryTypeDef],
    ) -> SQSBatchClientDeleteResults:
        results: SQSBatchClientDeleteResults = {
            "Successful": [],
            "Failed": [],
        }

        pending = tuple(entries)
        delay_gen = _full_jitter_backoff(20, 2)
        attempts = 0
        while pending:
            batch = pending[:10]
            response = await asyncio.to_thread(
                self.client.delete_message_batch,
                QueueUrl=queue_url,
                Entries=batch,
            )

            results["Successful"].extend(response.get("Successful", []))
            results["Failed"].extend(result for result in response.get("Failed", []) if result["SenderFault"])
            errors = {result["Id"]: result for result in response.get("Failed", []) if not result["SenderFault"]}

            if errors:
                attempts += 1
                if attempts < self.max_attempts:
                    delay = next(delay_gen)
                    await asyncio.sleep(delay)
                else:
                    results["Failed"].extend(errors.values())
                    break

            retryable = tuple(entry for entry in batch if entry["Id"] in errors)
            pending = retryable + pending[len(batch) :]

        return results
