"""
Consumes DAG-trigger requests from RabbitMQ and forwards them to Airflow via
the same trigger_dag() used by POST /dags/{dag_id}/trigger — lets an external
system enqueue a message instead of calling the REST API directly.

Expected message body (JSON):
    {"dag_id": "my_dag", "conf": {...}}   # conf is optional, forwarded as-is

Runs as a background asyncio task started from main.py's lifespan, using
aio-pika's robust connection so it reconnects automatically if RabbitMQ or
the network drops. Disabled entirely unless RABBITMQ_URL is configured.
"""

import asyncio
import json
import logging

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

import airflow_client as af
from config import RABBITMQ_QUEUE, RABBITMQ_URL

log = logging.getLogger(__name__)

_connection: AbstractRobustConnection | None = None
_consumer_task: asyncio.Task | None = None


async def _handle_message(message: AbstractIncomingMessage) -> None:
    # Failures (bad JSON, missing dag_id, Airflow rejecting the trigger) are
    # logged and the message is dropped (acked) rather than requeued, so a
    # single bad/poison message can't loop forever. Configure a dead-letter
    # exchange on the queue itself if you want failed messages retained for
    # inspection/retry instead of silently dropped.
    async with message.process():
        try:
            payload = json.loads(message.body)
        except json.JSONDecodeError:
            log.error("[rabbitmq] dropping malformed (non-JSON) message: %r", message.body[:200])
            return

        dag_id = payload.get("dag_id")
        conf = payload.get("conf", {})
        if not dag_id:
            log.error("[rabbitmq] dropping message with no dag_id: %r", payload)
            return

        try:
            result = await af.trigger_dag(dag_id, conf)
            log.info("[rabbitmq] triggered dag_id=%s run_id=%s", dag_id, result.get("dag_run_id"))
        except Exception:
            log.exception("[rabbitmq] failed to trigger dag_id=%s", dag_id)


async def _consume() -> None:
    global _connection
    _connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with _connection:
        channel = await _connection.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue(RABBITMQ_QUEUE, durable=True)
        log.info("[rabbitmq] listening on queue '%s'", RABBITMQ_QUEUE)
        await queue.consume(_handle_message)
        await asyncio.Future()  # run until cancelled by stop()


def start() -> None:
    """Start the consumer as a background task, if RABBITMQ_URL is configured."""
    global _consumer_task
    if not RABBITMQ_URL:
        log.info("[rabbitmq] RABBITMQ_URL not set — dag-trigger consumer disabled")
        return
    _consumer_task = asyncio.create_task(_consume())


async def stop() -> None:
    if _consumer_task:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
    if _connection:
        await _connection.close()