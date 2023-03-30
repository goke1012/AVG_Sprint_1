#!/usr/bin/env python

import pika
from etc.logger import get_logger

# Konstanen
HOST = 'localhost'
PORT = 5672
HARDWARE_QUEUE_NAME = 'hardware_Q'
HARDWARE_ROUTING_KEY = 'hardware'
SOFTWARE_QUEUE_NAME = 'software_Q'
SOFTWARE_ROUTING_KEY = 'software'
EXCHANGE_NAME = 'bestellungen'
EXCHANGE_TYPE = 'topic'

# Logger wird erzeugt und eingestellt
LOGGER = get_logger()

# Verbindung aufbauen
def get_connection(host=HOST, port=PORT):
  LOGGER.debug('get_connection: host=%s, port=%s', host, port)
  connection = None
  try:  
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port))
  except Exception as e:
    LOGGER.warning('Something went wrong creating a connection')
    LOGGER.debug('Connection failed due to Exception: %s', e)

  # Wenn keine Verbindung zum Server afgebaut werden konnte wird das Programm beendet
  if connection is None:
    LOGGER.debug('Connection failed (host= %s, port=%s)', host, port)
    print('Es konnte keine Verbindung zum RabbitMQ-Server hergestellt werden.')
    exit(1)
  return connection

# Einen Kommunikationskanal über eine vorhandene Verbindung bereitstellen
# Über die Connection wird auf einen Channel zugegriffen
# Queues werden falls nicht vorhanden angelegt und ansonsten für die Sendung später hinterlegt. Genauso der Exchange
def get_channel(connection):
  LOGGER.debug('get_channel: connection=%s', connection)
  channel = connection.channel()
  channel.queue_declare(queue=HARDWARE_QUEUE_NAME, durable=True)
  channel.queue_declare(queue=SOFTWARE_QUEUE_NAME, durable=True)
  channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE, durable=True)
  channel.queue_bind(exchange=EXCHANGE_NAME, queue=HARDWARE_QUEUE_NAME, routing_key=HARDWARE_ROUTING_KEY)
  channel.queue_bind(exchange=EXCHANGE_NAME, queue=SOFTWARE_QUEUE_NAME, routing_key=SOFTWARE_ROUTING_KEY)
  return channel