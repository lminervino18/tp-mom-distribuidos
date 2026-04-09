from __future__ import annotations

import pika
from pika.exceptions import (
    AMQPConnectionError,
    AMQPError,
    ChannelWrongStateError,
    ConnectionWrongStateError,
    StreamLostError,
)

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

_PREFETCH_COUNT = 1
_PERSISTENT_MESSAGE_PROPERTIES = pika.BasicProperties(delivery_mode=2)
_DISCONNECT_EXCEPTIONS = (
    AMQPConnectionError,
    ChannelWrongStateError,
    ConnectionWrongStateError,
    StreamLostError,
)


def _raise_runtime_error(exc: Exception) -> None:
    if isinstance(exc, _DISCONNECT_EXCEPTIONS):
        raise MessageMiddlewareDisconnectedError() from exc
    raise MessageMiddlewareMessageError() from exc


def _build_ack_handlers(channel, delivery_tag):
    handled = False

    def ack():
        nonlocal handled
        if handled:
            return
        try:
            channel.basic_ack(delivery_tag=delivery_tag)
            handled = True
        except Exception as exc:  # pragma: no cover
            _raise_runtime_error(exc)

    def nack():
        nonlocal handled
        if handled:
            return
        try:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            handled = True
        except Exception as exc:  # pragma: no cover
            _raise_runtime_error(exc)

    return ack, nack


class _RabbitMQMiddlewareBase:
    def _init_runtime_state(self) -> None:
        self.connection = None
        self.channel = None
        self._closed = False
        self._consuming = False
        self._consumer_tag = None

    def _connect(self, host: str) -> None:
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()
        except Exception as exc:
            self._best_effort_cleanup()
            _raise_runtime_error(exc)

    def _ensure_usable(self) -> None:
        if self._closed:
            raise MessageMiddlewareDisconnectedError()

        if self.connection is None or self.channel is None:
            raise MessageMiddlewareDisconnectedError()

        if not self.connection.is_open or not self.channel.is_open:
            raise MessageMiddlewareDisconnectedError()

    def _prepare_consumer(self, queue_name: str, on_message_callback) -> None:
        if self._consuming:
            raise MessageMiddlewareMessageError()

        self._ensure_usable()

        def internal_callback(ch, method, properties, body):
            ack, nack = _build_ack_handlers(ch, method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_qos(prefetch_count=_PREFETCH_COUNT)
            self._consumer_tag = self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=internal_callback,
                auto_ack=False,
            )
        except Exception as exc:
            _raise_runtime_error(exc)

    def _run_consumer_loop(self) -> None:
        self._consuming = True
        try:
            self.channel.start_consuming()
        except Exception as exc:
            if isinstance(exc, AMQPError):
                _raise_runtime_error(exc)
            raise
        finally:
            self._consuming = False
            self._consumer_tag = None

    def stop_consuming(self) -> None:
        if self._closed or not self._consuming or self._consumer_tag is None:
            return

        self._ensure_usable()

        try:
            self.channel.stop_consuming()
        except Exception as exc:
            _raise_runtime_error(exc)

    def close(self) -> None:
        if self._closed:
            return

        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception as exc:
            raise MessageMiddlewareCloseError() from exc
        finally:
            self._closed = True
            self._consuming = False
            self._consumer_tag = None

    def _best_effort_cleanup(self) -> None:
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass

        try:
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass


class MessageMiddlewareQueueRabbitMQ(
    _RabbitMQMiddlewareBase, MessageMiddlewareQueue
):
    def __init__(self, host, queue_name):
        self._init_runtime_state()
        self.queue_name = queue_name

        self._connect(host)

        try:
            self.channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception as exc:
            self._best_effort_cleanup()
            _raise_runtime_error(exc)

    def start_consuming(self, on_message_callback):
        self._prepare_consumer(self.queue_name, on_message_callback)
        self._run_consumer_loop()

    def send(self, message):
        self._ensure_usable()

        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=_PERSISTENT_MESSAGE_PROPERTIES,
            )
        except Exception as exc:
            _raise_runtime_error(exc)


class MessageMiddlewareExchangeRabbitMQ(
    _RabbitMQMiddlewareBase, MessageMiddlewareExchange
):
    def __init__(self, host, exchange_name, routing_keys):
        self._init_runtime_state()
        self.exchange_name = exchange_name
        self.routing_keys = list(routing_keys)
        self.queue_name = None
        self._consumer_queue_initialized = False

        self._connect(host)

        try:
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type="topic",
                durable=True,
            )
        except Exception as exc:
            self._best_effort_cleanup()
            _raise_runtime_error(exc)

    def _ensure_consumer_queue(self) -> None:
        if self._consumer_queue_initialized:
            return

        if not self.routing_keys:
            raise MessageMiddlewareMessageError()

        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )

            self._consumer_queue_initialized = True
        except Exception as exc:
            _raise_runtime_error(exc)

    def start_consuming(self, on_message_callback):
        self._ensure_usable()
        self._ensure_consumer_queue()

        if self.queue_name is None:
            raise MessageMiddlewareMessageError()

        self._prepare_consumer(self.queue_name, on_message_callback)
        self._run_consumer_loop()

    def send(self, message):
        if not self.routing_keys:
            raise MessageMiddlewareMessageError()

        self._ensure_usable()

        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=_PERSISTENT_MESSAGE_PROPERTIES,
                )
        except Exception as exc:
            _raise_runtime_error(exc)