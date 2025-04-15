"""
Middleware component for logging AMQP message processing.

This middleware logs message details and processing time for each request that passes
through the middleware chain. It captures:
- Message ID
- Request body
- Routing key
- Processing duration

Example:
    ```
    logger = logging.getLogger('my_service')
    middleware = LoggerMiddlware(logger)
    # Add to middleware chain
    manager.add_middleware(middleware)
    ```
"""

import time
from wasla.middleware_interface import MiddlewareInterface
from wasla.request import Request
from logging import Logger


class LoggerMiddleware(MiddlewareInterface):
    """
    Middleware that provides logging capabilities for message processing.
    
    This middleware logs the details of each message as it enters the processing
    chain and records the time taken to process each message.
    
    Attributes:
        logger: Logger instance to use for message logging
    """

    def __init__(self, logger: Logger):
        """
        Initialize the logger middleware.
        
        Args:
            logger: Logger instance to use for message logging
            
        Note:
            The logger should be configured with appropriate handlers and formatters
            before being passed to this middleware.
        """
        super().__init__()
        self.logger = logger

    async def handle(self, request: Request, next):
        """
        Log request details and processing time.
        
        This method logs the incoming message details, measures the processing
        time, and logs the completion time after the message has been processed
        by the rest of the middleware chain.
        
        Args:
            request: The request being processed
            next: Function to call the next middleware
            
        Returns:
            None
            
        Note:
            Timing includes processing by all subsequent middleware in the chain
        """
        # Log incoming message details
        self.logger.info(
            "Event %s: Request: %s, RoutingKey: %s",
            request.message_id,
            request.body,
            request.routing_key,
        )
        
        # Track processing time
        start_time = time.time()
        await next()
        
        # Log completion time
        self.logger.info(
            "Event %s: Time Taken: %s Seconds",
            request.message_id,
            time.time() - start_time,
        )
        return
