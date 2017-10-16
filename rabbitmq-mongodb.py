#!/usr/bin/env python
__author__ = 'Riccardo Pozza, r.pozza@surrey.ac.uk'

import logging
import logging.handlers
import sys

import json

import argparse

import pika

import pymongo

LOG_FILENAME = "/var/log/rabbitmq-mongodb.log"
JSON_CONFIG_LOCATION = "/usr/local/bin/mongologger/myconfig.json"

#class for logging to files
class MyLogger(object):
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.rstrip() != "":
            self.logger.log(self.level, message.rstrip())


class MongoDBConsumer(object):
    """Consumer handling RabbitMQ channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    EXCHANGE = 'json_log'
    EXCHANGE_TYPE = 'fanout'
    QUEUE = 'mongodb-storage'
    ROUTING_KEY = None
    SENSORS_LWM2M = {'/3301/0': 'luminosity', '/3303/0': 'temperature', '/3304/0': 'humidity', '/3324/0': 'loudness',
                    '/3325/0': 'dust', '/3330/0': 'ranging', '/3348/0': 'gesture'}

    def __init__(self, amqp_url, db_url):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._db = pymongo.MongoClient(db_url)

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        print "Connecting to " + str(self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection objectretries_number in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        print "Connection opened"
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        print "Adding connection close callback"
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            print "Connection closed, reopening in 5 seconds: (" + str(reply_code) + ") " + str(reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        print "Creating a new channel"
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        print "Channel opened"
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        print "Adding channel close callback"
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        print "Channel " + str(channel) + " was closed: (" + str(reply_code) + ") " + str(reply_text)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        print "Declaring exchange " + str(exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        print "Exchange declared"
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        print "Declaring queue " + str(queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        print "Binding " + str(self.EXCHANGE) + " to " + str(self.QUEUE) + " with " + str(self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        print "Queue bound"
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by firs%(filename)st calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        print "Issuing consumer related RPC commands"
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.QUEUE)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        print "Adding consumer cancellation callback"
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        print "Consumer was cancelled remotely, shutting down: " + str(method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        try:
            #parsing
            json_parsed = json.loads(body)
            for i in json_parsed['val']['resources']:
                if i['id'] == 5700 or i['id'] == 5547:
                    eggId = str(json_parsed['ep'])
                    variable = self.SENSORS_LWM2M[str(json_parsed['pth'])]
                    if i['id'] == 5547:
                        reading = float(i['value'])
                    if i['id'] == 5700:
                        reading = float(i['value'])
                    timestampISO = str(json_parsed['ts'])
            data_json = {'feature': variable, 'device':eggId, 'readings':reading, 'timestamp':timestampISO}
            #inserting
            self._db.deskegg_database.deskegg_collection.insert_one(json_parsed)
            self._db.iotegg.measurements.insert_one(data_json)
        except Exception, e:
            print "Error Processing:" + str(e)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            print "Sending a Basic.Cancel RPC command to RabbitMQ"
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        print "RabbitMQ acknowledged the cancellation of the consumer"
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        print "Closing the channel"
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        print "Stopping"
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        print "Stopped"

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        print "Closing connection"
        self._connection.close()

def main():
    # set up parsing log filename for multiple processes
    global LOG_FILENAME
    ID = '0'
    parser = argparse.ArgumentParser(description="RabbitMQ to MongoDB Storage Logging Service")
    parser.add_argument("-l", "--log", help="file to write log to (default '" + LOG_FILENAME + "')")
    parser.add_argument("-i", "--id", help="id of the file (default '" + ID + "')")
    args = parser.parse_args()
    if args.log:
        LOG_FILENAME = args.log
    if args.id:
        ID = args.id
    EXTRA = {'app_id': str(ID)}

    # Setting LOG options
    LOGGER = logging.getLogger(__name__)
    LOG_LEVEL = logging.INFO # setting log level to INFO but could be also "DEBUG" or "WARNING"
    LOGGER.setLevel(LOG_LEVEL)
    HANDLER = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="midnight", backupCount=3) # set where we're logging and how
    LOG_FORMAT = ('%(asctime)s %(levelname)s %(app_id)s %(name)s %(funcName)s %(lineno)d: %(message)s') # set format of logging
    FORMATTER = logging.Formatter(LOG_FORMAT)
    HANDLER.setFormatter(FORMATTER)
    LOGGER.addHandler(HANDLER)

    LOGGER = logging.LoggerAdapter(LOGGER,EXTRA)

    sys.stdout = MyLogger(LOGGER, logging.INFO) # replace stdout and stderr with custom logger
    sys.stderr = MyLogger(LOGGER, logging.ERROR)

    # Setting RABBITMQ options
    with open(JSON_CONFIG_LOCATION) as config_file:
        config_data = json.load(config_file)
    RABBIT_HOST = str(config_data["RabbitHost"])
    RABBIT_PORT = str(config_data["RabbitPort"])
    RABBIT_USER = str(config_data["RabbitUsername"])
    RABBIT_PASSWD = str(config_data["RabbitPassword"])
    AMQP_URL = 'amqp://' + RABBIT_USER + ':' + RABBIT_PASSWD + '@'+ RABBIT_HOST + ':' + RABBIT_PORT + '/%2F'

    # Setting MONGODB database options
    DB_USER = str(config_data["MongoUser"])
    DB_PSWD = str(config_data["MongoPassword"])
    DB_HOST = str(config_data["MongoHost"])
    DB_PORT = str(config_data["MongoPort"])
    MONGO_URL = 'mongodb://' + DB_USER + ':' + DB_PSWD + '@' + DB_HOST + ':' + DB_PORT
    consumer = MongoDBConsumer(AMQP_URL, MONGO_URL)
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


if __name__ == '__main__':
    main()
