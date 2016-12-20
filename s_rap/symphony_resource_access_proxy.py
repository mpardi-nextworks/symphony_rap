from tornado.ioloop import IOLoop
from threading import Thread
from s_rap.s_controllers import res_access
import pika

import logging
log = logging.getLogger(__name__)


RAP_QUEUE = 'symphony-s_rap-queue'
RAP_EXCHANGE = "symphony-s_rap-exchange"
RAP_BINDINGS = {"get.symphony", "set.symphony", "history.symphony",
                "subscribe.symphony", "unsubscribe.symphony"}


def configure_queue(channel, handler):
    channel.exchange_declare(exchange=RAP_EXCHANGE,
                             exchange_type='direct', auto_delete=False, durable=False)
    result = channel.queue_declare(queue=RAP_QUEUE, auto_delete=True)
    queue_name = result.method.queue
    for binding_key in RAP_BINDINGS:
        channel.queue_bind(exchange=RAP_EXCHANGE,
                           queue=queue_name,
                           routing_key=binding_key)

    channel.basic_consume(handler.receive_message,
                          queue=queue_name,
                          no_ack=True)
    return queue_name


def make_application():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    resource_access = res_access.ResourceAccess
    queue_name = configure_queue(channel, resource_access)
    log.debug('RabbitMQ service configured, waiting for messages on queue %s for topic %s', queue_name, RAP_EXCHANGE)

    rabbit_thr = Thread(target=channel.start_consuming)
    rabbit_thr.start()

    log.info("Symphony Resource Access Proxy service started")


def main():
    make_application()
    IOLoop.instance().start()


if __name__ == "__main__":
    main()
