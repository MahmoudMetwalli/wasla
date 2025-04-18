# `wasla` Documentation

## Overview
Wasla was developed to facilitate the build of events based systems that uses RabbitMQ as the message broker,
wasla is built on [aio_pika](https://github.com/mosquito/aio-pika) to represent the core of RabbitMQ connection,
while wasla provides a mini framework for routing and middlewares.
With the right configuration and a well structured routing map, you can publish an event to many consumers
with just one line of code.

## Installation
```bash
pip install wasla
```

## Quick Start

### 1. Create a Consumer Service
```python
from wasla import Builder, Router
from pydantic import BaseModel

# Define your event schema
class OrderCreatedEvent(BaseModel):
    order_id: str
    amount: float

# Create router with optional prefix
order_router = Router(prefix="orders")

@order_router.route("created", OrderCreatedEvent)
async def handle_order_created(event: OrderCreatedEvent):
    print(f"Processing order {event.order_id}")

# Configure the consumer
builder = Builder(
    routing_key="orders.#",  # Base routing key
    queue_name="order_processor",
    concurrency_limit=5    # Max parallel messages
)

# Add components
builder.include_router(order_router)
```

### 2. Connect to RabbitMQ
```python
import aio_pika

async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()
    exchange = await channel.declare_exchange("events", ExchangeType.TOPIC)
    
    # Configure builder
    builder.amqp_channel = channel
    builder.exchange = exchange
    
    # Start consuming
    await builder.run()

asyncio.run(main())
```

## Core Features

### Routing System
```python
# With schema validation
@router.route("payment.completed", PaymentEvent)
async def handle_payment(event: PaymentEvent):
    pass

# Without validation (accepts any payload)
@router.route("log")
async def handle_logs(event):
    pass
```

### Middleware Pipeline
```python
# Custom middleware example
class MetricsMiddleware(MiddlewareInterface):
    async def handle(self, request, next):
        start = time.time()
        await next()
        print(f"Request took {time.time()-start:.2f}s")

builder.add_middleware(MetricsMiddleware())
```

### Error Handling
- Automatic retries (3 attempts by default)


### Logging Customization
```python
# Customize logger (automatically created if not set)
builder.logger = my_custom_logger
```

### Manual Queue Binding
```python
queue = await channel.declare_queue("custom-queue")
builder.queue = queue  # Override auto-created queue
```


## Best Practices

1. **Schema Validation**: Always define Pydantic models for important events
2. **Concurrency**: Set reasonable limits based on your workload
3. **Monitoring**: Check DLQ regularly for failed messages
4. **Logging**: Use the built-in structured logger or integrate with your existing system

## Troubleshooting

**Common Issues**:
- `ValueError: Exchange must be TOPIC type` → Ensure your exchange is declared as `ExchangeType.TOPIC`
- `Message validation failed` → Verify your Pydantic schemas match the event payloads
- `Queue binding failed` → Check your routing key patterns (wildcards: `#`, `*`)

For additional support, please [open an issue](https://github.com/MahmoudMetwalli/wasla/issues).

## License
Licensed under the [Apache 2.0 License](LICENSE).  
See [NOTICE](NOTICE) for third-party attributions.
