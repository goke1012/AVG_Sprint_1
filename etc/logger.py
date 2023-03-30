import logging
import inspect
from os import sep as path_seperator

# Hier kan man bei Bedarf den Loglevel für alle Logger setzen
# Ist für die Simulation selbst eher unpraktsch, da wenn man ihn setzt durch "pika" eine extrem unübersichtliche Ausgabe entsteht
# Aber wenn man Logging will, kann man es damit aktivieren
def set_loglevel(log_level=logging.DEBUG):
  logging.basicConfig(level=log_level)

# Logger wird erzeugt
# Der Logger hat den selben Namen, wie auch die aufrufende Datei
def get_logger():
  caller_filename = inspect.stack()[1].filename
  logger = logging.getLogger(caller_filename.split(path_seperator).pop().split('.')[0])
  return logger
