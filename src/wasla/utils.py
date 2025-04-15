"""
Utility functions for AMQP message handling and Pydantic model serialization.

This module provides helper functions for building AMQP messages from Pydantic models
with support for all AMQP message properties and proper validation.
"""

from aio_pika import Message, DeliveryMode
from pydantic import BaseModel
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

    This function takes a Pydantic model instance and converts it to an AMQP message,
    allowing customization of all standard AMQP message properties. It performs
    validation on critical parameters like delivery mode and priority.

    Args:
        obj: Pydantic model to serialize into message body
        encoding: Character encoding for message body (default: utf-8)
        headers: Custom message headers dictionary
        content_type: MIME content type (default: application/json)
        content_encoding: MIME content encoding (defaults to encoding value)
        delivery_mode: PERSISTENT (2) or TRANSIENT (1) delivery
        priority: Message priority level (0-9)
        correlation_id: Correlation ID for request-reply pattern
        reply_to: Reply queue name for request-reply pattern
        expiration: Message expiration timestamp or TTL
        message_id: Unique message identifier
        timestamp: Message creation timestamp
        type: Message type name (defaults to model class name)
        user_id: Creating user identifier
        app_id: Creating application identifier

    Returns:
        Message: An aio_pika Message instance ready for publishing

    Raises:
        ValueError: If delivery_mode is invalid (not 1 or 2)
        ValueError: If priority is invalid (not 0-9)
        
    Example:
        ```
        class UserCreated(BaseModel):
            user_id: str
            email: str

        event = UserCreated(user_id="123", email="user@example.com")
        message = build_message(
            event,
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=5
        )
        await topic_exchange.publish(message, routing_key="users.created")
        ```
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
        content_type=content_type,
        content_encoding=content_encoding,
        delivery_mode=delivery_mode,
        priority=priority,
        correlation_id=correlation_id,
        reply_to=reply_to,
        expiration=expiration,
        message_id=message_id,
        timestamp=timestamp,
        type=type,
        user_id=user_id,
        app_id=app_id,
    )
