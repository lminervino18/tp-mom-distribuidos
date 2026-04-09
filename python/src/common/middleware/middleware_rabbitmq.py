from __future__ import annotations

import time

import pika
from pika.exceptions import (
    AMQPConnectionError,
    AMQPError,
    ChannelWrongStateError,
    ConnectionWrongStateError,
    NackError,
    StreamLostError,
    UnroutableError,
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

_HEARTBEAT_SECONDS = 30
_BLOCKED_CONNECTION_TIMEOUT_SECONDS = 30
_SOCKET_TIMEOUT_SECONDS = 5
_STACK_TIMEOUT_SECONDS = 10
_RECONNECT_BACKOFF_SECONDS = (0.2, 0.5, 1.0)

_DISCONNECT_EXCEPTIONS = (
    AMQPConnectionError,
    ChannelWrongStateError,
    ConnectionWrongStateError,
    StreamLostError,
)
_MESSAGE_EXCEPTIONS = (
    NackError,
    UnroutableError,
)


def _is_disconnect_error(exc: Exception) -> bool:
    return isinstance(exc, _DISCONNECT_EXCEPTIONS)


def _raise_runtime_error(exc: Exception) -> None:
    if isinstance(exc, _DISCONNECT_EXCEPTIONS):
        raise MessageMiddlewareDisconnectedError() from exc
    if isinstance(exc, _MESSAGE_EXCEPTIONS):
        raise MessageMiddlewareMessageError() from exc
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
        except Exception as exc:  
            _raise_runtime_error(exc)

    def nack():
        nonlocal handled
        if handled:
            return
        try:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            handled = True
        except Exception as exc:  
            _raise_runtime_error(exc)

    return ack, nack


class _RabbitMQMiddlewareBase:
    def _init_runtime_state(self, host: str) -> None:
        self.host = host
        self.connection = None
        self.channel = None
        self._publish_channel = None
        self._closed = False
        self._consuming = False
        self._consumer_tag = None

    def _build_connection_parameters(self) -> pika.ConnectionParameters:
        # Heartbeats, blocked timeout y un backoff corto para reducir bloqueos
        return pika.ConnectionParameters(
            host=self.host,
            heartbeat=_HEARTBEAT_SECONDS,
            blocked_connection_timeout=_BLOCKED_CONNECTION_TIMEOUT_SECONDS,
            socket_timeout=_SOCKET_TIMEOUT_SECONDS,
            stack_timeout=_STACK_TIMEOUT_SECONDS,
            connection_attempts=1,
        )

    def _connect(self) -> None:
        last_exc = None
        for attempt, delay in enumerate((0.0, *_RECONNECT_BACKOFF_SECONDS)):
            if delay > 0:
                time.sleep(delay)

            try:
                self.connection = pika.BlockingConnection(
                    self._build_connection_parameters()
                )
                self.channel = self.connection.channel()
                self._publish_channel = None
                return
            except Exception as exc:
                last_exc = exc
                self._best_effort_cleanup()

        assert last_exc is not None
        _raise_runtime_error(last_exc)

    def _redeclare_topology(self) -> None:
        raise NotImplementedError

    def _reconnect(self) -> None:
        assert not self._closed
        if self._closed:
            raise MessageMiddlewareDisconnectedError()

        self._best_effort_cleanup()
        self._connect()
        self._redeclare_topology()

    def _ensure_connection(self) -> None:
        assert not self._closed
        if self._closed:
            raise MessageMiddlewareDisconnectedError()

        if self.connection is None or not self.connection.is_open:
            self._reconnect()

    def _ensure_consumer_channel(self) -> None:
        self._ensure_connection()

        if self.channel is not None and self.channel.is_open:
            return

        try:
            assert self.connection is not None
            self.channel = self.connection.channel()
        except Exception as exc:
            if _is_disconnect_error(exc):
                self._reconnect()
                return
            _raise_runtime_error(exc)

    def _ensure_publish_channel(self) -> None:
        self._ensure_connection()

        if self._publish_channel is not None and self._publish_channel.is_open:
            return

        try:
            # Se usa un canal dedicado de publicación para no mezclar confirms y publish
            assert self.connection is not None
            self._publish_channel = self.connection.channel()
            self._publish_channel.confirm_delivery()
        except Exception as exc:
            if _is_disconnect_error(exc):
                self._reconnect()
                assert self.connection is not None
                self._publish_channel = self.connection.channel()
                self._publish_channel.confirm_delivery()
                return
            _raise_runtime_error(exc)

    def _call_with_disconnect_retry(self, operation):
        try:
            return operation()
        except Exception as exc:
            if _is_disconnect_error(exc):
                self._reconnect()
                try:
                    return operation()
                except Exception as retry_exc:
                    _raise_runtime_error(retry_exc)
            _raise_runtime_error(exc)

    def _prepare_consumer(self, queue_name: str, on_message_callback) -> None:
        assert not self._consuming
        if self._consuming:
            raise MessageMiddlewareMessageError()

        self._ensure_consumer_channel()

        def internal_callback(ch, method, properties, body):
            ack, nack = _build_ack_handlers(ch, method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            # Prefetch 1 prioriza fairnes (decisión de trhoughput)
            assert self.channel is not None
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
            assert self.channel is not None
            self.channel.start_consuming()
        except Exception as exc:
            if isinstance(exc, AMQPError):
                _raise_runtime_error(exc)
            raise
        finally:
            self._consuming = False
            self._consumer_tag = None

    def _publish(self, exchange: str, routing_key: str, message) -> None:
        def operation():
            self._ensure_publish_channel()
            # mandatory=True hace visible el caso unroutable
            assert self._publish_channel is not None
            self._publish_channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message,
                properties=_PERSISTENT_MESSAGE_PROPERTIES,
                mandatory=True,
            )

        # Este retry simple deja la publicacion con una semantica at_least_once
        self._call_with_disconnect_retry(operation)

    def stop_consuming(self) -> None:
        if self._closed or not self._consuming or self._consumer_tag is None:
            return

        self._ensure_consumer_channel()

        try:
            assert self.channel is not None
            self.channel.stop_consuming()
        except Exception as exc:
            _raise_runtime_error(exc)

    def close(self) -> None:
        if self._closed:
            return

        close_error = None

        # Se cierran explícitamente ambos canales y la conexión
        for resource in (self._publish_channel, self.channel, self.connection):
            if resource is None:
                continue

            try:
                if resource.is_open:
                    resource.close()
            except Exception as exc:
                if close_error is None:
                    close_error = exc

        self._closed = True
        self._consuming = False
        self._consumer_tag = None
        self._publish_channel = None
        self.channel = None
        self.connection = None

        if close_error is not None:
            raise MessageMiddlewareCloseError() from close_error

    def _best_effort_cleanup(self) -> None:
        # El cleanup intenta cerrar todo lo que haya abierto o parcialmente abierto
        for resource in (self._publish_channel, self.channel, self.connection):
            if resource is None:
                continue

            try:
                if resource.is_open:
                    resource.close()
            except Exception:
                pass

        self._publish_channel = None
        self.channel = None
        self.connection = None
        self._consumer_tag = None
        self._consuming = False


class MessageMiddlewareQueueRabbitMQ(
    _RabbitMQMiddlewareBase, MessageMiddlewareQueue
):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self._init_runtime_state(host)
        self._connect()
        self._redeclare_topology()

    def _redeclare_topology(self) -> None:
        self._ensure_consumer_channel()
        assert self.channel is not None
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        self._prepare_consumer(self.queue_name, on_message_callback)
        self._run_consumer_loop()

    def send(self, message):
        self._publish(exchange="", routing_key=self.queue_name, message=message)


class MessageMiddlewareExchangeRabbitMQ(
    _RabbitMQMiddlewareBase, MessageMiddlewareExchange
):
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = list(routing_keys)
        self.queue_name = None
        self._consumer_queue_initialized = False

        self._init_runtime_state(host)
        self._connect()
        self._redeclare_topology()

    def _declare_exchange(self) -> None:
        self._ensure_consumer_channel()
        assert self.channel is not None
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="direct",
            durable=True,
        )

    def _create_and_bind_consumer_queue(self) -> None:
        assert self.routing_keys
        if not self.routing_keys:
            raise MessageMiddlewareMessageError()

        self._ensure_consumer_channel()
        assert self.channel is not None
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue

        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

        self._consumer_queue_initialized = True

    def _redeclare_topology(self) -> None:
        self._declare_exchange()

        if self._consumer_queue_initialized:
            self._create_and_bind_consumer_queue()

    def _ensure_consumer_queue(self) -> None:
        if self._consumer_queue_initialized and self.queue_name is not None:
            return

        self._create_and_bind_consumer_queue()

    def start_consuming(self, on_message_callback):
        self._ensure_consumer_queue()

        assert self.queue_name is not None
        if self.queue_name is None:
            raise MessageMiddlewareMessageError()

        self._prepare_consumer(self.queue_name, on_message_callback)
        self._run_consumer_loop()

    def send(self, message):
        assert self.routing_keys
        if not self.routing_keys:
            raise MessageMiddlewareMessageError()

        for routing_key in self.routing_keys:
            self._publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                message=message,
            )