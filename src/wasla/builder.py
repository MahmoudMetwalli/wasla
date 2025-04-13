"""Builder Module"""
import asyncio
import logging
from logging import Logger
import colorlog
from aio_pika.abc import AbstractIncomingMessage
from aio_pika import Channel, Exchange, ExchangeType, Message, DeliveryMode, Queue
from wasla.request import Request
from wasla.middleware_manager import MiddlewareManager
from wasla.middleware_interface import MiddlewareInterface
from wasla.routing_middleware import RoutingMiddleware
from wasla.logger_middleware import LoggerMiddlware
from wasla.router import Router

class Builder:
    def __init__(self,  routing_key: str, queue_name: str | None = None, concurrency_limit: int = 10):
        self.__queue_name = queue_name
        self.__routing_key = routing_key
        self.__concurrency_limit = concurrency_limit
        self.__routers = []
        self.__routes = []
        self.__middlewares = []
        self._queue = None
        self.__semaphore = None
        self._amqp_channel = None
        self._exchange = None
        self._logger = None
        self.__middleware_manager = MiddlewareManager()
        self.__routing_middleware = RoutingMiddleware(self.__routes)

    @property
    def queue(self) -> Queue:
        """Get the queue name"""
        return self._queue
    @queue.setter
    def queue(self, queue: Queue):
        """Set the queue with validation"""
        if not isinstance(queue, Queue):
            raise TypeError("Queue name must be an instance of aio_pika.Queue")
        if self._amqp_channel and queue.channel != self._amqp_channel:
            raise ValueError("Queue must belong to the configured AMQP channel")
        self._queue = queue

    @property
    def amqp_channel(self) -> Channel:
        """Get the AMQP channel"""
        return self._amqp_channel

    @amqp_channel.setter
    def amqp_channel(self, value: Channel):
        """Set the AMQP channel with validation"""
        if not isinstance(value, Channel):
            raise TypeError("AMQP channel must be an instance of aio_pika.Channel")
        if value.is_closed:
            raise ValueError("AMQP channel is closed")
        self._amqp_channel = value

    @property
    def exchange(self) -> Exchange:
        """Get the exchange"""
        return self._exchange

    @exchange.setter
    def exchange(self, value: Exchange):
        """Set the exchange with validation"""
        if not isinstance(value, Exchange):
            raise TypeError("Exchange must be an instance of aio_pika.Exchange")
        if value._type != ExchangeType.TOPIC:
            raise ValueError("Exchange type must be TOPIC")
        if self._amqp_channel and value.channel != self._amqp_channel:
            raise ValueError("Exchange must belong to the configured AMQP channel")
        self._exchange = value

    @property
    def logger(self) -> Logger:
        """Get the logger"""
        return self._logger

    @logger.setter
    def logger(self, value: Logger):
        """Set the logger with validation"""
        if value is None:
            raise ValueError("Logger cannot be None")
        if not isinstance(value, Logger):
            raise TypeError("Logger must be an instance of logging.Logger")
        self._logger = value

    async def __set_queue(self):
        """Set the queue with the exchange and routing key"""
        if self._exchange is None:
            raise ValueError("Exchange is required")
        if self._exchange.channel != self._amqp_channel:
            raise ValueError("Exchange must belong to the configured AMQP channel")
        
        # Declaring queue
        if isinstance(self._queue, Queue):
            if self._queue.channel != self._amqp_channel:
                raise ValueError("Queue must belong to the configured AMQP channel")
            await self._queue.bind(self._exchange, routing_key=f"#.{self.__routing_key}.#")
        else:
            if self._amqp_channel is None:
                raise ValueError("AMQP channel is required or manually set a queue")
            # If queue is not set, create one
            if self.__queue_name is None:
                raise ValueError("Manually set a queue, or provide a queue name to automatically create one")
            # Create a new queue with the given name
            self.__queue = await self._amqp_channel.declare_queue(
                self.__queue_name, durable=True,
            )
            await self.__queue.bind(self._exchange, routing_key=f"{self.__routing_key}")

    async def __set_semaphore(self):
        self.__semaphore = asyncio.Semaphore(self.__concurrency_limit)  # Limit concurrent tasks

    async def __set_routes(self):
        for router in self.__routers:
            for route in router.routes:
                if router.prefix != "":
                    route["routing_key"] = router.prefix + "." + route["routing_key"]
                self.__routes.append(route)
        seen_routes = set()
        duplicates = []
        invalid_routes = []
        for route in self.__routes:
            routing_key = route.get("routing_key")
            if routing_key in seen_routes:
                duplicates.append(routing_key)
            else:
                seen_routes.add(routing_key)
            if not (self.__routing_key in routing_key):
                invalid_routes.append(route)
        if len(duplicates) != 0:
            raise Exception("Duplicated Routes Are not Allowed")
        if len(invalid_routes) != 0:
            raise Exception(f"Routing Keys Should Include The Service Routing Key: {self.__routing_key}")

    async def __consume(self):
        """Consume messages with async processing and manual acknowledgment"""
        self._logger.info(f"Listening to queue: {self.__queue_name} with routing key: {self.__routing_key}")
        async with self.__queue.iterator() as iterator:
            message: AbstractIncomingMessage
            async for message in iterator:
            # Don't use message.process() to handle ack manually
                try:
                    async with self.__semaphore:
                        # Create and track the task
                        task = asyncio.create_task(self.__message_handler(message))
                        # Add done callback for acknowledgment
                        task.add_done_callback(
                            lambda t: asyncio.create_task(self.__handle_completion(message, t))
                        )
                except Exception as e:
                    self._logger.error(f"Failed to create task for message:{message.message_id}: {e}")
                    await message.reject(requeue=True)

    async def __handle_completion(self, message: AbstractIncomingMessage, task: asyncio.Task):
        """Handle task completion with retry counting"""
        try:
            await task
            await message.ack()
        except Exception as e:
            retry_count = int(message.headers.get('x-retry-count', 0))
            self._logger.error(f"Message {message.message_id} failed, will undergo retry, {e}", exc_info=True)
            # Check if retry count is less than max retries
            if retry_count < 3:  # Max retries
                # Republish to end of queue with increased retry count
                message_retry = Message(
                message.body,
                delivery_mode=DeliveryMode.PERSISTENT,
                type=str,
                headers={'x-retry-count': retry_count + 1})
                await self._exchange.publish(
                    message_retry,
                    routing_key=message.routing_key,
                )
                await message.ack()  # Ack original message
            else:
                # Move to dead letter queue or log permanent failure
                self._logger.error(f"Message {message.message_id} failed after {retry_count} retries")
                await message.reject(requeue=False)

    async def __get_logger(self, service_name: str) -> Logger:
    # Create a custom color formatter for console output
        color_formatter = colorlog.ColoredFormatter(
                fmt='%(purple)s%(asctime)s%(reset)s - %(blue)s%(name)s%(reset)s - %(log_color)s%(levelname)s%(reset)s - %(message_log_color)s%(message)s%(reset)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                log_colors={
                    'DEBUG':    'cyan',
                    'INFO':     'green',
                    'WARNING': 'yellow',
                    'ERROR':    'red',
                    'CRITICAL': 'red,bg_white',
                },
                secondary_log_colors={
                    'message': {
                        'DEBUG':    'white',
                        'INFO':     'white', 
                        'WARNING': 'yellow',
                        'ERROR':    'red',
                        'CRITICAL': 'red'
                    }
                },
                style='%'
            )

        # Regular formatter for file output (without colors)
        file_formatter = logging.Formatter(
                fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

        # Configure console handler with colors
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_formatter)

        # Configure file handler (without colors)
        file_handler = logging.FileHandler(f"{service_name}.log")
        file_handler.setFormatter(file_formatter)

        # Get logger instance
        logger = logging.getLogger(f"{service_name}")
        logger.setLevel(logging.INFO)

        # Remove any existing handlers and add our custom handlers
        logger.handlers.clear()
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        # Prevent logger from propagating to root logger
        logger.propagate = False

        return logger

    def include_router(self, router: Router):
        self.__routers.append(router)

    async def __message_handler(self, message: AbstractIncomingMessage):
        request = Request(message)
        await self.__middleware_manager.execute(request)

    def add_middleware(self, middleware: MiddlewareInterface):
        """Add middleware to the builder"""
        self.__middlewares.append(middleware)

    async def __activate_middleware(self):
        """Activate middleware"""
        for middleware in self.__middlewares:
            if isinstance(middleware, MiddlewareInterface):
                self.__middleware_manager.add_middleware(middleware)
            else:
                raise TypeError("Middleware must be an instance of MiddlewareInterface")

    async def run(self):
        """Run the builder"""
        if self._logger is None:
            self._logger = await self.__get_logger(self.__queue_name)
        await self.__set_queue()
        await self.__set_semaphore()
        await self.__set_routes()
        self.__middleware_manager.add_middleware(LoggerMiddlware(self._logger))
        await self.__activate_middleware()
        self.__middleware_manager.add_middleware(self.__routing_middleware)
        await self.__consume()
