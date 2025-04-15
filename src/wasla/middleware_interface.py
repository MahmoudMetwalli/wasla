"""
Abstract base class defining the interface for middleware components.

This module provides the contract that all middleware components must implement.
It ensures consistent behavior across different middleware implementations by
enforcing a standard interface for request handling and chain processing.

Example:
    ```
    class LoggerMiddleware(MiddlewareInterface):
        async def handle(self, request, next):
            print(f"Processing request: {request.routing_key}")
            await next()
            print("Request processed")
    ```
"""

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable


class MiddlewareInterface(ABC):
    """
    Abstract interface for middleware components in the processing chain.
    
    This class defines the contract that all middleware must follow. Each middleware
    can process requests and control the flow of the middleware chain by choosing
    whether to call the next middleware.
    
    Attributes:
        next: Reference to the next middleware in the chain
    """
    
    def __init__(self):
        """Initialize middleware with no next component."""
        self.next = None

    @abstractmethod
    async def handle(self, request: Any, next: Callable[[], Awaitable[None]]) -> None:
        """
        Process the request and optionally pass it to the next middleware.
        
        This method must be implemented by concrete middleware classes to define
        their specific processing logic.
        
        Args:
            request: The request object to process
            next: Callable that triggers the next middleware in the chain
            
        Returns:
            None
            
        Note:
            - Implementations should call `await next()` to continue the chain
            - Not calling next() stops the chain execution at this middleware
        """
        pass
