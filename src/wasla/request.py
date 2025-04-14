from typing import Any, TypeVar, Generic
from datetime import datetime
from aio_pika import Message
from aio_pika.abc import HeadersType
from aio_pika import DeliveryMode
import json

T = TypeVar("T")


class Request(Generic[T]):
    """
    Wrapper for AMQP messages with typed attributes and generic payload type.

    Attributes:
        body: Parsed message body of type T
        routing_key: Message routing key
        message_id: Unique message identifier
        correlation_id: Request correlation ID
        reply_to: Reply queue name
        content_type: Message content type
        content_encoding: Message encoding
        headers: Message headers
        delivery_mode: Message persistence mode
        priority: Message priority
        timestamp: Message creation time
        expiration: Message expiration
        type: Message type
        user_id: Message sender ID
        app_id: Sending application ID
    """

    def __init__(self, message: Message) -> None:
        body = self._parse_message(message.body)
        self.body: dict[str, Any] | str = body
        self.routing_key: str = message.routing_key
        self.message_id: str | None = message.message_id
        self.correlation_id: str | None = message.correlation_id
        self.reply_to: str | None = message.reply_to
        self.content_type: str | None = message.content_type
        self.content_encoding: str | None = message.content_encoding
        self.headers: HeadersType | None = message.headers
        self.delivery_mode: DeliveryMode | None = message.delivery_mode
        self.priority: int | None = message.priority
        self.timestamp: datetime | None = message.timestamp
        self.expiration: datetime | None = message.expiration
        self.type: str | None = message.type
        self.user_id: str | None = message.user_id
        self.app_id: str | None = message.app_id

    def _parse_message(self, body: bytes) -> dict[str, Any] | str:
        """Parse message body from bytes to dict or string."""
        try:
            return json.loads(body.decode())
        except json.JSONDecodeError:
            return body.decode()
