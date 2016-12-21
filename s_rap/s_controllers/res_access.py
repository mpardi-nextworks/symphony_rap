import json
from tornado.ioloop import IOLoop
from s_rap.platform_id import SYMPHONY_PLATFORM_ID
import logging
log = logging.getLogger(__name__)


class ResourceAccess:
    def __init__(self):
        self.io_loop = IOLoop.current()

    @staticmethod
    def receive_message(self, ch, method, properties, body):
        try:
            key = method.routing_key
            log.debug("Resource access message received.\nTopic: %s\nBody: %s", key, body)
            decoded = body.decode("utf-8")
            data = json.loads(decoded)
            action = data.get("action")
            platform = data.get("platformId")
            assert platform == SYMPHONY_PLATFORM_ID
            if action == "get":
                print "get"
            if action == "history":
                print "history"
            if action == "set":
                print "set"
            if action == "subscribe":
                print "subscribe"
            if action == "unsubscribe":
                print "unsubscribe"

            # self.io_loop.add_callback(self.notify_monitoring, resource_id)
        except (AssertionError, ValueError) as err:
            log.info(err)
