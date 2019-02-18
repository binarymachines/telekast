#!/usr/bin/env python


from snap import common


class PipelineRecordHeaderFactory(object):
    def __init__(self, *field_names):
        self.kwreader = common.KeywordArgReader(*field_names)

    def create(self, validator=None, **kwargs):
        # TODO: add validation logic
        data = {}
        self.kwreader.read(**kwargs)
        for field in self.kwreader.required_keywords:
            data[field] = self.kwreader.get_value(field)
        return data


    @property
    def field_names(self):
        return self.kwreader.required_keywords

    
class PipelineRecordFactory(object):
    def __init__(self, **kwargs):
        kwreader = common.KeywordArgReader('payload_field_name')
        kwreader.read(**kwargs)
        self.payload_field_name = kwreader.get_value('payload_field_name')


    def create(self, header_dict, validator=None, **kwargs):
        record = {}
        record.update(header_dict)
        record[self.payload_field_name] = kwargs
        return record




class IngestRecordBuilder(object):
    def __init__(self, record_header, **kwargs):
        self.header = record_header
        self.source_data = kwargs or {}


    def add_field(self, name, value):
        self.source_data[name] = value
        return self


    def add_fields(self, **kwargs):
        self.source_data.update(kwargs)
        return self


    def build(self):
        result = {}
        result['header'] = self.header.data()
        result['body'] = self.source_data
        return result



class KafkaNode(object):
    def __init__(self, host, port=9092):
        self._host = host
        self._port = port

    def __call__(self):
        return '%s:%s' % (self._host, self._port)


def json_serializer(value):
    return json.dumps(value).encode('utf-8')


def json_deserializer(value):
    return json.loads(value.decode('utf-8'))


class KafkaMessageHeader(object):
    def __init__(self, header_dict):
        self.__dict__ = header_dict


class KafkaCluster(object):
    def __init__(self, nodes=[], **kwargs):
        self._nodes = nodes


    def add_node(self, kafka_node):
        new_nodes = copy.deepcopy(self._nodes)
        new_nodes.append(kafka_node)
        return KafkaCluster(new_nodes)


    @property
    def nodes(self):
        return ','.join([n() for n in self._nodes])


    @property
    def node_array(self):
        return self._nodes



class KafkaOffsetManagementContext(object):
    def __init__(self, kafka_cluster, topic, **kwargs):
        #NOTE: keep this code for now
        '''
        self._client = KafkaClient(bootstrap_servers=kafka_cluster.nodes)
        self._metadata = self._client.cluster
        '''
        self._partition_table = {}

        consumer_group = kwargs.get('consumer_group', 'test_group')
        kreader = KafkaIngestRecordReader(topic, kafka_cluster.node_array, consumer_group)

        print('### partitions for topic %s: %s' % (topic, kreader.consumer.partitions))
        part1 = kreader.consumer.partitions[0]
        print('### last committed offset for first partition: %s' % kreader.consumer.committed(part1))


    @property
    def partitions(self):
        # TODO: figure out how to do this
        return None


    def get_offset_for_partition(self, partition_id):
        '''NOTE TO SARAH: this should stay more or less as-is, because
        however we retrieve the partition & offset data from the cluster,
        we should store it in a dictionary'''

        offset = self._partition_table.get(partition_id)
        if offset is None:
            raise NoSuchPartitionException(partition_id)
        return offset



class KafkaLoader(object):
    def __init__(self, topic, kafka_ingest_record_writer, **kwargs):
        self._topic = topic
        self._kwriter = kafka_ingest_record_writer
        self._header = IngestRecordHeader(**kwargs)


    def load(self, data):
        msg_builder = IngestRecordBuilder(self._header)
        for key, value in data.items():
            msg_builder.add_field(key, value)
        ingest_record = msg_builder.build()

        log.debug('writing record to Kafka topic "%s":' % self._topic)
        log.debug(ingest_record)
        self._kwriter.write(self._topic, ingest_record)



class KafkaIngestRecordWriter(object):
    def __init__(self, kafka_node_array, serializer=json_serializer):
        self.producer = KafkaProducer(bootstrap_servers=','.join([n() for n in kafka_node_array]),
                                      value_serializer=serializer,
                                      acks=1,
                                      api_version=(0,10))

        error_handler = ConsoleErrorHandler()
        self._promise_queue = IngestWritePromiseQueue(error_handler, debug_mode=True)


    def write(self, topic, ingest_record):
        future = self.producer.send(topic, ingest_record)
        self._promise_queue.append(future)
        return future


    def sync(self, timeout=0):
        self.producer.flush(timeout or None)


    @property
    def promise_queue_size(self):
        return self._promise_queue.size


    def process_write_promise_queue(self):
        self._promise_queue.run()
        return self._promise_queue.errors

