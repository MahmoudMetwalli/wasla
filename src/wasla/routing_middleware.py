import inspect
from wasla.middleware_interface import MiddlewareInterface
from wasla.request import Request
from pydantic import BaseModel
from typing import Any


class RoutingMiddleware(MiddlewareInterface):
    def __init__(self, router):
        self.__router = router

    async def handle(self, request: Request, next):
        for route in self.__router:
            if request.routing_key == route["routing_key"]:
                event = await self.validate_chema(route["event_schema"], request)
                # Get handler signature
                sig = inspect.signature(route["handler"])
                params = sig.parameters

                # Check if handler uses positional or keyword arguments
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

                # Call handler with both args and kwargs
                await route["handler"](*args, **kwargs)
                return

    async def validate_chema(self, schema: BaseModel, request: Request) -> BaseModel:
        """
        Validate and convert request message to Pydantic model instance.

        Args:
            schema: Pydantic model class
            request: Request object containing message data

        Returns:
            Instantiated Pydantic model
        """
        if isinstance(request.body, dict):
            return schema.model_validate(request.body)
        return schema.model_validate_json(request.body)
