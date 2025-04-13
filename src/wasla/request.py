from aio_pika import Message
import json

class Request:
	def __init__(self, message: Message):
		self.message = json.loads(message.body.decode())
		self.routing_key = message.routing_key
		self.message_id = message.message_id
