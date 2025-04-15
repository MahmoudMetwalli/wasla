"""
Router module for handling AMQP message routing and event validation.

This module provides routing functionality for AMQP messages with Pydantic model validation.
It allows defining routes with routing keys and associated event schemas for validation.
"""

from functools import wraps
from pydantic import BaseModel


class DynamicAcceptModel(BaseModel):
    """
    A flexible Pydantic model that accepts any data structure without validation.
    
    This model is used as a default schema when no specific validation is needed.
    It allows arbitrary fields to be present in the incoming data.
    """

    class Config:
        extra = "allow"  # Allows arbitrary fields


class Router:
    """
    AMQP message router that manages routing keys and their handlers.
    
    Attributes:
        routes (list): List of registered routes with their handlers and schemas
        prefix (str): Optional prefix added to all routing keys
        fixed_parameters: Additional parameters to be passed to handlers
    """

    def __init__(self, prefix: str = ""):
        """
        Initialize a new router instance.
        
        Args:
            prefix (str): Optional prefix to prepend to all routing keys
        """
        self.routes = []
        self.prefix = prefix

    async def set_fixed_parameters(self, fixed_parameters):
        """
        Set parameters that should be passed to all handlers.
        
        Args:
            fixed_parameters: Parameters to be included in handler calls
        """
        self.fixed_parameters = fixed_parameters

    def route(self, routing_key: str, event_schema: type[BaseModel] = DynamicAcceptModel):
        """
        Decorator for registering message handlers with routing keys and schemas.
        
        Args:
            routing_key (str): AMQP routing key for message matching
            event_schema (type[BaseModel]): Pydantic model for message validation
            
        Returns:
            callable: Decorator function for the handler
            
        Raises:
            ValueError: If handler is not callable or routing key is not a string
        """
        def decorator(handler):
            if not callable(handler):
                raise ValueError("Handler must be a callable")
            if not isinstance(routing_key, str):
                raise ValueError("Routing key must be a string")
            if not issubclass(event_schema, BaseModel):
                raise ValueError("Event schema must be a subclass of pydantic BaseModel")

            @wraps(handler)
            async def wrapper(*args, **kwargs):
                """Wraps the handler to preserve its metadata"""
                return await handler(*args, **kwargs)

            self.routes.append({
                "routing_key": routing_key,
                "handler": wrapper,
                "event_schema": event_schema,
            })
            return wrapper

        return decorator
