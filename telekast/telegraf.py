#!/usr/bin/env python

import os
import sys
import threading
import time
import datetime
import json
import docopt
import yaml
from snap import common
from mercury import sqldbx as sqlx

import logging
from logging import Formatter
import copy

from kafka import KafkaProducer, KafkaConsumer, KafkaClient
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.common import OffsetRequestPayload

from sqlalchemy import Integer, String, DateTime, text, and_
from sqlalchemy import Integer, String, DateTime, Float, text
from sqlalchemy.sql import bindparam

from sqlalchemy.sql import bindparam

log = logging.getLogger(__name__)




class ObjectstoreDBRelay(DataRelay):
    def __init__(self, **kwargs):
        DataRelay.__init__(self, **kwargs)
        kwreader = common.KeywordArgReader('db', 'tablespec')
        kwreader.read(**kwargs)
        self.database = kwreader.get_value('db')        
        self.tablespec = kwreader.get_value('tablespec')
        self._insert_sql = text(self.tablespec.insert_statement_template)
        
        
    def _send(self, src_message_header, data, **kwargs):
        '''execute insert statement against objectstore DB'''

        rec_data = self.tablespec.convert_data(data)
        rec_data['generation'] = 0
        rec_data['correction_id'] = None
        insert_statement = self._insert_sql.bindparams(**rec_data)

        with sqlx.txn_scope(self.database) as session:
            session.execute(insert_statement)




class K2Relay(DataRelay):
    def __init__(self, target_topic, kafka_ingest_log_writer, **kwargs):
        DataRelay.__init__(self, **kwargs)
        self._target_log_writer = kafka_ingest_log_writer
        self._target_topic = target_topic


    def _send(self, kafka_message):
        self._target_log_writer.write(self._target_topic, kafka_message.value)



class BulkTransferAgent(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('local_temp_dir',
                                           'src_filename',
                                           'src_file_header',
                                           'src_file_delimiter')

        kwreader.read(kwargs)
        self._local_temp_directory = kwreader.get_value('local_temp_dir')
        self._source_filename = kwreader.get_value('src_filename')
        self._source_file_header = kwreader.get_value('src_file_header')
        self._source_file_delimiter = kwreader.get_value('src_file_delimiter')
        self._svc_registry = None
        self._transfer_functions = {}


    def register_service_objects(self, name, service_object_config):
        service_object_tbl = snap.initialize_services(service_object_config)
        self._svc_registry = common.ServiceObjectRegistry(service_object_tbl)


    def register_transfer_function(self, operation_name, transfer_function):
        self._transfer_functions[operation_name] = transfer_function


    def transfer(self, operation_name, **kwargs):
        transfer_func = self._transfer_functions.get(operation_name)
        if not transfer_func:
            raise UnregisteredTransferFunctionException(operation_name)

        transfer_func(self._svc_registry, **kwargs)



class ConsoleErrorHandler(object):
    def __init__(self):
        pass

    def handle_error(self, exception_obj):
        print('error in telegraf data operation:')
        print(str(exception_obj))



class SentryErrorHandler(object):
    def __init__(self, sentry_dsn):
        self.client = Client(sentry_dsn)


    def handle_error(self, exception_obj):
        self.client.send(str(exception_obj))



class IngestWritePromiseQueue(threading.Thread):
    '''Queues up the Future objects returned from KafkaProducer.send() calls
       and then handles the results of failed requests in a background thread
    '''

    def __init__(self, error_handler, futures = [], **kwargs):
        threading.Thread.__init__(self)
        self._futures = futures
        self._error_handler = error_handler
        self._errors = []
        self._sentry_logger = kwargs.get('sentry_logger')
        self._debug_mode = False
        if kwargs.get('debug_mode'):
            self._debug_mode = True

        self._future_retry_wait_time = 0.01


    def append(self, future):
        self._futures.append(future)
        return self
               
               
    def append_all(self, future_array):                
        self._futures.extend(future_array)
        return self
        


    def process_entry(self, f):
        result = {
            'status': 'ok',
            'message': ''
        }
        if not f.succeeded:
            result['status'] = 'error'
            result['message'] = f.exception.message
            log.error('write promise failed with exception: %s' % str(f.exception))
            self._sentry_logger.error('write promise failed with exception: %s' % str(f.exception))
            self._error_handler.handle_error(f.exception)
        return result


    def run(self):
        log.info('processing %d Futures...' % len(self._futures))
        results = []
        for f in self._futures:
            while not f.is_done:
                time.sleep(self._future_retry_wait_time)
            results.append(self.process_entry(f))
        self._errors = [r for r in results if r['status'] is not 'ok']
        log.info('all futures processed.')


    @property
    def size(self):
        return len(self._futures)


    @property
    def errors(self):
        return self._errors





