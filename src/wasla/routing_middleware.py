"""
Routing middleware for handling AMQP message routing and parameter injection.

This middleware handles:
- Route matching based on routing keys
- Message validation using Pydantic schemas
- Dynamic parameter injection into handlers
- Support for both positional and keyword arguments
"""

import inspect
from wasla.middleware_interface import MiddlewareInterface
from wasla.request import Request
from pydantic import BaseModel
from typing import Any


class RoutingMiddleware(MiddlewareInterface):
    """
    Middleware that routes messages to appropriate handlers and injects dependencies.
    
    This middleware:
    - Matches messages to routes using routing keys
    - Validates message data against Pydantic schemas
    - Injects validated events and request objects into handlers
    - Supports both positional and keyword argument styles
    
    Attributes:
        __router: List of route definitions containing handlers and schemas
    """

    def __init__(self, router):
        """
        Initialize the routing middleware.
        
        Args:
            router: List of route definitions to handle
        """
        self.__router = router

    async def handle(self, request: Request, next):
        """
        Handle incoming requests by routing them to appropriate handlers.
        
        This method:
        1. Matches the request to a route
        2. Validates the message data
        3. Injects dependencies into the handler
        4. Calls the handler with appropriate arguments
        
        Args:
            request: The incoming AMQP request
            next: Next middleware in chain
            
        Returns:
            None
        """
        for route in self.__router:
            if request.routing_key == route["routing_key"]:
                event = await self.validate_chema(route["event_schema"], request)
                sig = inspect.signature(route["handler"])
                params = sig.parameters

                args: list[Any] = []
                kwargs: dict[str, Any] = {}

                # Handle both positional and keyword arguments
                for param_name, param in params.items():
                    if param.kind == param.POSITIONAL_OR_KEYWORD:
                        if param_name == "event":
                            args.append(event)
                        elif param_name == "request":
                            args.append(request)
                    elif param.kind == param.KEYWORD_ONLY:
                        if param_name == "event":
                            kwargs["event"] = event
                        elif param_name == "request":
                            kwargs["request"] = request

                await route["handler"](*args, **kwargs)
                return

    async def validate_chema(self, schema: type[BaseModel], request: Request) -> BaseModel:
        """
        Validate and convert request message to Pydantic model instance.
        
        This method handles both dictionary and JSON string inputs,
        validating them against the provided Pydantic schema.

        Args:
            schema: Pydantic model class to validate against
            request: Request object containing message data

        Returns:
            BaseModel: Validated Pydantic model instance

        Raises:
            ValidationError: If message data doesn't match schema
        """
        if isinstance(request.body, dict):
            return schema.model_validate(request.body)
        return schema.model_validate_json(request.body)
