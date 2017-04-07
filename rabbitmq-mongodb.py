#!/usr/bin/env python
__author__ = 'Riccardo Pozza, r.pozza@surrey.ac.uk'

import logging
import logging.handlers
import argparse
import sys
import pika
import pymongo
import json


def main():
    # Defaults
    LOG_FILENAME = "/var/log/rabbitmq-mongodb.log"
    LOG_LEVEL = logging.INFO  # i.e. could be also "DEBUG" or "WARNING"

    # Define and parse command line arguments
    parser = argparse.ArgumentParser(description="RabbitMQ to MongoDB Storage Logging Service")
    parser.add_argument("-l", "--log", help="file to write log to (default '" + LOG_FILENAME + "')")

    # If the log file is specified on the command line then override the default
    args = parser.parse_args()
    if args.log:
        LOG_FILENAME = args.log

    # Configure logging to log to a file, making a new file at midnight and keeping the last 3 day's data
    # Give the logger a unique name (good practice)
    logger = logging.getLogger(__name__)
    # Set the log level to LOG_LEVEL
    logger.setLevel(LOG_LEVEL)
    # Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
    handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="midnight", backupCount=3)
    # Format each log message like this
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    # Attach the formatter to the handler
    handler.setFormatter(formatter)
    # Attach the handler to the logger
    logger.addHandler(handler)

    # Replace stdout with logging to file at INFO level
    sys.stdout = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)

    # RabbitMQ Credentials and Connection to Broker
    with open('/usr/local/bin/mongologger/myconfig.json') as config_file:
        config_data = json.load(config_file)

    print "Connection to MongoDB Service DataBase and Collection"
    mongoEndpoint = 'mongodb://' + config_data["MongoUser"] + ':' + config_data["MongoPassword"]
    mongoEndpoint += '@' + config_data["MongoHost"] + ':' + str(config_data["MongoPort"])
    client = pymongo.MongoClient(mongoEndpoint)
    _database = client.deskegg_database
    _collection = _database.deskegg_collection
    print "Connected to MongoDB Service"

    # RabbitMQ Credentials and Connection to Broker
    print "Connection to RabbitMQ Message Broker"
    usercred = pika.PlainCredentials(config_data["RabbitUsername"], config_data["RabbitPassword"])
    connection = pika.BlockingConnection(pika.ConnectionParameters(config_data["RabbitHost"],config_data["RabbitPort"],'/',usercred))
    channel = connection.channel()
    print "Connected to RabbitMQ Message Broker"

    # need to declare exchange also in subscriber
    channel.exchange_declare(exchange='json_log', type='fanout')

    print "Creating a temporary Message Queue"
    # temporary queue name, closed when disconnected
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    # bind to the queue considered attached to logs exchange
    channel.queue_bind(exchange='json_log', queue=queue_name)
    print "Bound to the temporary Message Queue"

    print "Waiting for messages to Consume!"
    channel.basic_consume(lambda ch, method, properties, body:callback_ext_arguments(
                                 ch, method, properties, body,collection=_collection), queue=queue_name, no_ack=True)
    channel.start_consuming()

def callback_ext_arguments(ch, method, properties, body, collection):
    #body contains the json doc
    try:
        json_parsed = json.loads(body)
    except:
        return
    try:
        post_id = collection.insert_one(json_parsed).inserted_id
    except:
        return

# Make a class we can use to capture stdout and sterr in the log
class MyLogger(object):
        def __init__(self, logger, level):
                """Needs a logger and a logger level."""
                self.logger = logger
                self.level = level

        def write(self, message):
                # Only log if there is a message (not just a new line)
                if message.rstrip() != "":
                        self.logger.log(self.level, message.rstrip())

if __name__ == '__main__':
    main()
