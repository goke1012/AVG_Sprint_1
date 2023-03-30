#!/usr/bin/env python

from  etc.bestellungen import BESTELLUNGEN
from etc.logger import get_logger
from etc.rabbitmq import get_connection, get_channel
from pika import BasicProperties
import json

# Logger wird erzeugt und eingestellt
LOGGER = get_logger()

# Bestellungen werden importiert aus der Datei "bestellungen.py"
# Der Routing-Key wird anhand der Bestellun gesetzt. Anhand des Keys wird die Nachricht später vom Exchange an die passende Queue weitergeleitet
# Die Bestellungen werden vor dem versenden jeweils in Bytecode umgewandelt
def send_orders(channel, bestellungen=BESTELLUNGEN):
  LOGGER.debug('send_orders: channel=%s, bestellungen=%s', channel, bestellungen)
  for bestellung in bestellungen:
    routing_key = bestellung["bestelltyp"]
    channel.basic_publish(
      exchange= 'bestellungen',
      routing_key= routing_key,
      properties=BasicProperties(delivery_mode=2),  # Damit werden Nachrichten auch nach Neustart von RabbitMQ erhalten bleiben
      body = json.dumps(bestellung),
    )
    print(f'Nachricht gesendet: "{bestellung}" mit RoutingKey "{routing_key}"')

# Main-Hethode
def main():
  connection = get_connection()
  channel = get_channel(connection=connection)
  try:
    send_orders(channel=channel)
  except Exception as e:
    LOGGER.debug('Sending of orders failed due to to Exception: %s', e)
    print('Der Producer konnte die Nachrichten nicht (vollständig) versenden')
  finally:
    connection.close()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    LOGGER.info('Exiting Programm via keyboard interrupt')
    print('Programm wird beendet...')
    exit(0)