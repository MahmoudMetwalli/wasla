from functools import wraps
from pydantic import BaseModel


class DynamicAcceptModel(BaseModel):
    """Accepts any data (no validation)."""

    class Config:
        extra = "allow"  # Allows arbitrary fields


class Router:
    def __init__(self, prefix: str = ""):
        self.routes = []
        self.prefix = prefix

    async def set_fixed_parameters(self, fixed_parameters):
        self.fixed_parameters = fixed_parameters

    def route(self, routing_key: str, event_schema: BaseModel = DynamicAcceptModel):
        def decorator(handler):
            @wraps(handler)
            async def wrapper(*args, **kwargs):
                return await handler(*args, **kwargs)

            self.routes.append(
                {
                    "routing_key": routing_key,
                    "handler": wrapper,
                    "event_schema": event_schema,
                }
            )
            return wrapper

        return decorator
