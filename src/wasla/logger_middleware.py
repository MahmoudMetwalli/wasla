import time
from wasla.middleware_interface import MiddlewareInterface
from wasla.request import Request


class LoggerMiddlware(MiddlewareInterface):

    def __init__(self, logger):
        """Initialize the middleware with a logger."""
        self.logger = logger

    async def handle(self, request: Request, next):
        self.logger.info(
            "Event %s: Request: %s, RoutingKey: %s",
            request.message_id,
            request.body,
            request.routing_key,
        )
        start_time = time.time()
        await next()
        self.logger.info(
            "Event %s: Time Taken: %s Seconds",
            request.message_id,
            time.time() - start_time,
        )
        return
