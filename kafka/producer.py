import atexit
import contextlib
import itertools
import struct
import threading

import kafka.io
import kafka.request_type


class Producer(kafka.io.IO):
    """Sends data to a `Kafka <http://sna-projects.com/kafka/>`_ broker.

    :param topic: Topic to produce to.
    :param partition: Broker partition to produce to.
    :param host: Kafka host.
    :param port: Kafka port.
    :param max_message_sz: Max byte size of produce request. [default: 1MB]

    """
    PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

    def __init__(self, topic, partition=0, host='localhost', port=9092, max_message_sz=1048576):
        kafka.io.IO.__init__(self, host, port)

        topic_str = str(topic) # This handles the case of using an object with a
                               # __str__ method.
        # The topic must be ascii, not unicode.
        if type(topic_str) == unicode:
            topic_str = unicodedata.normalize('NFKD', topic_str)
            topic_str = topic_str.encode('ascii', 'ignore')
        self.topic = topic_str
        max_message_sz = max_message_sz
        self.partition = partition
        self.connect()

    def _pack_payload(self, messages):
        """Pack a list of messages into a sendable buffer.

        :param msgs: The packed messages to send.
        :param size: The size (bytes) of all the `messages` to send.

        """
        payload = ''.join(messages)
        payload_sz = len(payload)
        topic_sz = len(self.topic)
        # Create the request as::
        #   <REQUEST_ID: short>
        #   <TOPIC_SIZE: short>
        #   <TOPIC: bytes>
        #   <PARTITION: int>
        #   <BUFFER_SIZE: int>
        #   <BUFFER: bytes>
        return struct.pack(
            '>HH%dsii%ds' % (topic_sz, payload_sz),
            self.PRODUCE_REQUEST_ID,
            topic_sz,
            self.topic,
            self.partition,
            payload_sz,
            payload
        )

    def _pack_kafka_message(self, payload):
        """Pack a payload in a format kafka expects."""
        return struct.pack('>i%ds' % len(payload), len(payload), payload)

    def encode_request(self, messages):
        """Encode a sequence of messages for sending to a kafka broker.

        Encoding a request can yeild multiple kafka messages if the payloads exceed
        the maximum produce size.

        :param messages: An iterable of :class:`Message <kafka.message>` objecjts.
        :rtype: A generator of packed kafka messages ready for sending.

        """
        encoded_msgs = []
        encoded_msgs_sz = 0
        for message in messages:
            encoded = message.encode()
            encoded_sz = len(encoded)
            if encoded_sz + encoded_msgs_sz > self.max_message_sz:
                yield self._pack_kafka_message(self._pack_payload(encoded_msgs))
                encoded_msgs = []
                encoded_msgs_sz = 0
            msg = struct.pack('>i%ds' % encoded_sz, encoded_sz, encoded)
            encoded_msgs.append(msg)
            encoded_msgs_sz += encoded_sz
        if encoded_msgs:
            yield self._pack_kafka_message(self._pack_payload(encoded_msgs))

    def send(self, messages):
        """Send :class:`Message <kafka.message>` or sequence of `Messages`."""
        if isinstance(messages, kafka.message.Message):
            messages = [messages]
        for message in self.encode_request(messages):
            sent = self.write(message)
            if sent != len(message):
                raise IOError(
                    'Failed to send kafka message - sent %s/%s many bytes.' % (sent, len(message)))

    @contextlib.contextmanager
    def batch(self):
        """Send messages with an implict `send`."""
        messages = []
        yield(messages)
        self.send(messages)


class BatchProducer(Producer):
    """Batch messages to `Kafka <http://sna-projects.com/kafka/>`_ broker with periodic flushing.

    :param topic: Topic to produce to.
    :param batch_interval: Amount of time (seconds) to wait before sending.
    :param partition: Broker partition to produce to.
    :param host: Kafka host.
    :param port: Kafka port.

    """
    MAX_RESPAWNS = 5
    PRODUCE_REQUEST_ID = kafka.request_type.PRODUCE

    def __init__(self, topic, batch_interval, partition=0, host='localhost', port=9092):
        Producer.__init__(
            self, topic, partition=partition, host=host, port=port)
        self.batch_interval = batch_interval
        self._message_queue = []
        self.event = threading.Event()
        self.lock = threading.Lock()
        self.timer = None
        atexit.register(self.close)
        self.respawns = 0

    def check_timer(self):
        """Starts the flush timer and restarts it after forks."""
        if (self.timer and self.timer.is_alive()) or self.respawns > self.MAX_RESPAWNS:
            return
        self.respawns += 1
        self.timer = threading.Thread(target=self._interval_timer)
        self.timer.daemon = True
        self.timer.start()
        self.connect()

    def _interval_timer(self):
        """Flush the message queue every `batch_interval` seconds."""
        while not self.event.is_set():
            self.flush()
            self.event.wait(self.batch_interval)

    def enqueue(self, message):
        """Enqueue a message in the queue.

        .. note:: Messages are implicitly sent every `batch_interval` seconds.

        :param message: The message to queue for sending at next send interval.

        """
        with self.lock:
            self.check_timer()
            self._message_queue.append(message)

    def flush(self):
        """Send all messages in the queue now."""
        with self.lock:
            if len(self._message_queue) > 0:
                self.send(self._message_queue)
                # Reset the queue
                del self._message_queue[:]

    def close(self):
        """Shutdown the timer thread and flush the message queue."""
        self.event.set()
        self.flush()
        if self.timer:
            self.timer.join()
