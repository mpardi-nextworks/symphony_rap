import json
from tornado.ioloop import IOLoop

import logging
log = logging.getLogger(__name__)


class ResourceAccess:
    def __init__(self):
        self.io_loop = IOLoop.current()

    @staticmethod
    def receive_message(self, ch, method, properties, body):
        key = method.routing_key
        log.debug("Resource Registration message received.\nTopic: %s\nBody: %s", key, body)
        decoded = body.decode("utf-8")
        data = json.loads(decoded)

        #if not isinstance(action, str):
        #    "wrong value, it must be string"
        #    return

        if key == "get.symphony":
            print "get"
        if key == "history.symphony":
            print "history"
        if key == "set.symphony":
            print "set"
        if key == "subscribe.symphony":
            print "subscribe"
        if key == "unsubscribe.symphony":
            print "unsubscribe"

        #self.io_loop.add_callback(self.notify_monitoring, resource_id)

