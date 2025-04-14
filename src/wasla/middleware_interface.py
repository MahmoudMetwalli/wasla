from abc import ABC, abstractmethod


class MiddlewareInterface(ABC):
    @abstractmethod
    async def handle(self, request, next):
        pass
