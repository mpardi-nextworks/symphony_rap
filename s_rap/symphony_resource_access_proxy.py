import json
from tornado.ioloop import IOLoop
from threading import Thread
from s_rap.platform_id import SYMPHONY_PLATFORM_ID
from s_rap.s_controllers.res_access import ResourceAccess
import pika

import logging
log = logging.getLogger(__name__)


EXCHANGE_IN = "plugins-exchange"

QUEUE_IN = SYMPHONY_PLATFORM_ID + "-rap-platform-queue"
TOPICS_IN = {"get." + SYMPHONY_PLATFORM_ID, "set." + SYMPHONY_PLATFORM_ID, "history." + SYMPHONY_PLATFORM_ID,
             "subscribe." + SYMPHONY_PLATFORM_ID, "unsubscribe." + SYMPHONY_PLATFORM_ID}

EXCHANGE_OUT = "rap-exchange"
TOPICS_OUT = "symbiote.rap.add-plugin"


def configure_queue(channel, handler):
    channel.exchange_declare(exchange=EXCHANGE_IN, exchange_type='topic')
    result = channel.queue_declare(queue=QUEUE_IN, auto_delete=True)
    queue_name = result.method.queue
    for binding_key in TOPICS_IN:
        channel.queue_bind(exchange=EXCHANGE_IN,
                           queue=queue_name,
                           routing_key=binding_key)

    channel.basic_consume(handler.receive_message,
                          queue=queue_name,
                          no_ack=True)
    return queue_name


def register_plugin(channel):
    channel.exchange_declare(exchange=EXCHANGE_OUT, type='topic')
    payload = {
        "platformId": SYMPHONY_PLATFORM_ID,
    }
    message = json.dumps(payload)
    channel.basic_publish(exchange=EXCHANGE_OUT,
                          routing_key=TOPICS_OUT,
                          body=message)


def make_application():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    resource_access = ResourceAccess()
    configure_queue(channel, resource_access)
    register_plugin(channel)
    log.debug("RabbitMQ service configured")

    rabbit_thr = Thread(target=channel.start_consuming)
    rabbit_thr.start()

    log.info("Symphony Resource Access Proxy service started")


def main():
    make_application()
    IOLoop.instance().start()


if __name__ == "__main__":
    main()
