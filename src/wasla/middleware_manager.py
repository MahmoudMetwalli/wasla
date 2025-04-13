from wasla.middleware_interface import MiddlewareInterface


class MiddlewareManager:
    def __init__(self):
        self.__head = None
        self.__tail = None

    def add_middleware(self, middleware: MiddlewareInterface):
        if not self.__head:
            self.__head = middleware
        else:
            self.__tail.next = middleware
        self.__tail = middleware

    async def execute(self, request):
        async def run_middleware(middleware):
            if middleware:
                await middleware.handle(request, lambda: run_middleware(middleware.next))

        await run_middleware(self.__head)
