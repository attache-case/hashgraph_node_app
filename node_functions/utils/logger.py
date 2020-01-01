import datetime
import watchtower, logging

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('sample_logger')
_logger.addHandler(watchtower.CloudWatchLogHandler())

def info(message):
    _logger.info('[%s] %s' % (current_datetime(), message))

def error(message):
    _logger.error('[%s] %s' % (current_datetime(), message))

def current_datetime():
    return datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')