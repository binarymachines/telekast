#!/usr/bin/env python

from logging import Formatter
from snap import common
from raven import Client
from raven.handlers.logging import SentryHandler

log = logging.getLogger(__name__)


class TelegrafErrorHandler(object):
    def __init__(self, logging_level=logging.DEBUG, **kwargs):
        kwreader = common.KeywordArgReader('sentry_dsn')
        kwreader.read(**kwargs)
        
        sentry_dsn = kwargs.get_value('sentry_dsn')
        self._client = Client(sentry_dsn)
        sentry_handler = SentryHandler()
        sentry_handler.setLevel(logging_level)
        log.addHandler(sentry_handler)
