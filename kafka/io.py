import array
import errno
import socket

class IO(object):
  """ Base class for handling socket communication to the Kafka server. """

  def __init__(self, host='localhost', port=9092):
    self.socket = None

    #: Hostname to connect to.
    self.host   = host

    #: Port to connect to.
    self.port   = port

  def connect(self):
    """ Connect to the Kafka server. """

    self.socket = socket.socket()
    self.socket.connect((self.host, self.port))

  def reconnect(self):
    """ Reconnect to the Kafka server. """
    self.disconnect()
    self.connect()

  def disconnect(self):
    """ Disconnect from the remote server & close the socket. """
    try:
      self.socket.close()
    except IOError:
      pass
    finally:
      self.socket = None

  def read(self, length):
    """ Send a read request to the remote Kafka server. """

    # Create a character array to act as the buffer.
    buf         = bytearray(length)
    bytes_left  = length

    try:
      while bytes_left > 0:
        read_length = self.socket.recv_into(memoryview(buf)[length-bytes_left:], bytes_left)
        bytes_left -= read_length

    except errno.EAGAIN:
      self.disconnect()
      raise IOError, "Timeout reading from the socket."

    else:
      return str(buf)

  def write(self, data):
    """ Write `data` to the remote Kafka server. """

    if self.socket is None:
      self.connect()

    wrote_length = 0
    write_length = len(data)

    while write_length > wrote_length:
      remainder = data[wrote_length:]
      wrote_length += self.socket.send(remainder)

    return wrote_length
