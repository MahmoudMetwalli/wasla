from wasla.middleware_interface import MiddlewareInterface

class RoutingMiddleware(MiddlewareInterface):
    def __init__(self, router):
        self.__router = router

    async def handle(self, request, next):
        for route in self.__router:
            if request.routing_key == route["routing_key"]:
                await self.validate_chema(route["event_schema"], request)
                await route["handler"](request.message)
                return

    async def validate_chema(self, schema, request):
        schema(**request.message)
