#!/usr/bin/env python

import json
from etc.logger import get_logger
from etc.rabbitmq import get_connection, get_channel

# Logger wird erzeugt und eingestellt
LOGGER = get_logger()

# Gegebene Anzahl an Hardware-Consumern starten
def start_hardware_consumers(channel, count):
  LOGGER.debug('start_hardware_consumers: channel=%s count=%d', channel, count)
  def hardware_consumer(ch, method, properties, body):
    LOGGER.debug('hardware_consumer: ch=%s, method=%s, properties=%s, body=%s', ch, method, properties, body)
    bestellung = json.loads(body)
    print(f"Hardware bestellung eingegangen: {bestellung}")
  for i in range(count):
    channel.basic_consume(queue='hardware_Q', on_message_callback=hardware_consumer, auto_ack=True)

# Gegebene Anzahl an Software-Consumern starten
def start_software_consumers(channel, count):
  LOGGER.debug('start_software_consumers: channel=%s count=%d', channel, count)
  def software_consumer(ch, method, properties, body):
    LOGGER.debug('software_consumer: ch=%s, method=%s, properties=%s, body=%s', ch, method, properties, body)
    bestellung = json.loads(body)
    print(f"Software bestellung eingegangen: {bestellung}")
  for i in range(count):
    channel.basic_consume(queue='software_Q', on_message_callback=software_consumer, auto_ack=True)
    
# Main-Methode
def main():
    connection = get_connection()
    channel = get_channel(connection=connection)
    start_hardware_consumers(channel=channel, count=1)
    start_software_consumers(channel=channel, count=1)   
    
    print(' [*] Warte auf Bestellungen. To exit press CTRL+C')
    
    try:
      channel.start_consuming()
    except Exception as e:
      LOGGER.debug('Conection got closed due to to Exception: %s', e)
      print('Verbindung zum RabbitMQ-Server wurde unerwartet unterbrochen')
    finally:
      connection.close()

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    LOGGER.info('Exiting Programm via keyboard interrupt')
    print('Programm wird beendet...')
    exit(0)

