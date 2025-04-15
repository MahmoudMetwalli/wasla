"""
Middleware Manager for AMQP message processing pipeline.

This module implements a middleware chain pattern for processing AMQP messages.
It allows middleware components to be chained together and executed in sequence,
with each middleware having the ability to modify the request or control the flow
of execution.

Example:
    ```
    manager = MiddlewareManager()
    manager.add_middleware(LoggerMiddleware())
    manager.add_middleware(ValidationMiddleware())
    await manager.execute(request)
    ```
"""

from wasla.middleware_interface import MiddlewareInterface


class MiddlewareManager:
    """
    Manages a chain of middleware components for message processing.
    
    This class implements a linked list of middleware components where each middleware
    can process the request and decide whether to pass control to the next middleware
    in the chain.
    
    Attributes:
        __head: First middleware in the chain
        __tail: Last middleware in the chain
    """

    def __init__(self):
        """Initialize an empty middleware chain."""
        self.__head = None
        self.__tail = None

    def add_middleware(self, middleware: MiddlewareInterface):
        """
        Add a new middleware to the end of the chain.
        
        Args:
            middleware: The middleware instance to add
            
        Note:
            Middleware objects must implement the MiddlewareInterface
        """
        if not self.__head:
            self.__head = middleware
        else:
            self.__tail.next = middleware
        self.__tail = middleware

    async def execute(self, request):
        """
        Execute the middleware chain for a given request.
        
        This method starts the execution of the middleware chain by calling
        the first middleware in the sequence. Each middleware can then choose
        to call the next middleware in the chain.
        
        Args:
            request: The request object to process through the middleware chain
            
        Note:
            The execution stops if any middleware doesn't call the next middleware
        """
        async def run_middleware(middleware: MiddlewareInterface):
            """
            Recursive helper function to run each middleware.
            
            Args:
                middleware: The current middleware to execute
            """
            if middleware:
                await middleware.handle(
                    request, lambda: run_middleware(middleware.next)
                )

        await run_middleware(self.__head)
