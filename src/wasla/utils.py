from aio_pika import Message, DeliveryMode
from pydantic import BaseModel
from aio_pika import Message, DeliveryMode
from aio_pika.abc import HeadersType, DateType


def build_message(
    obj: BaseModel,
    *,
    encoding: str = "utf-8",
    headers: HeadersType | None = None,
    content_type: str | None = None,
    content_encoding: str | None = None,
    delivery_mode: DeliveryMode | int | None = None,
    priority: int | None = None,
    correlation_id: str | None = None,
    reply_to: str | None = None,
    expiration: DateType = None,
    message_id: str | None = None,
    timestamp: DateType = None,
    type: str | None = None,
    user_id: str | None = None,
    app_id: str | None = None,
) -> Message:
    """
    Encode a Pydantic model into an AMQP message with full parameter support.

    Args:
        obj: Pydantic model to serialize
        encoding: Character encoding for the message body
        headers: Message headers dictionary
        content_type: MIME content type
        content_encoding: MIME content encoding
        delivery_mode: Delivery mode (PERSISTENT or TRANSIENT)
        priority: Message priority (0-9)
        correlation_id: Correlation ID for RPC
        reply_to: Reply-to queue name
        expiration: Message expiration timestamp
        message_id: Unique message identifier
        timestamp: Message timestamp
        type: Message type name
        user_id: Creating user ID
        app_id: Creating application ID

    Returns:
        An AioPika Message instance
    """
    # Validate delivery mode
    if isinstance(delivery_mode, int) and delivery_mode not in (1, 2):
        raise ValueError("delivery_mode must be 1 (TRANSIENT) or 2 (PERSISTENT)")

    # Validate priority
    if priority is not None and not (0 <= priority <= 9):
        raise ValueError("priority must be between 0 and 9")

    # Convert Pydantic model to bytes
    body = obj.model_dump_json().encode(encoding)

    # Create message with validated parameters
    return Message(
        body=body,
        headers=headers,
        content_type=content_type or "application/json",
        content_encoding=content_encoding or encoding,
        delivery_mode=delivery_mode,
        priority=priority,
        correlation_id=correlation_id,
        reply_to=reply_to,
        expiration=expiration,
        message_id=message_id,
        timestamp=timestamp,
        type=type or obj.__class__.__name__,
        user_id=user_id,
        app_id=app_id,
    )
