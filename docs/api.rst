.. _api:

API
===

.. module:: aiothrift

This part of the documentation covers all the interfaces of aiothrift.  For
parts where aiothrift depends on external libraries, we document the most
important right here and provide links to the canonical documentation.


Application Object
------------------


.. autoclass:: ThriftConnection
   :members:

   .. automethod:: __init__

   .. attribute:: service

   .. attribute:: iprot

   .. attribute:: oprot

   .. attribute:: address

   .. attribute:: loop

   .. attribute:: timeout

Connection Pool
---------------

.. currentmodule:: aiothrift.pool

.. autoclass:: ThriftPool
   :members:
   :inherited-members:

   .. automethod:: __init__

   .. attribute:: service

      The thrift service name

   .. attribute:: address

      (host port) tuple

   .. attribute:: minsize

      minial connection number

   .. attribute:: maxsize

      maximal connection number

   .. attribute:: loop

    .. attribute:: timeout

protocol
--------

.. module:: aiothrift.protocol

.. autoclass:: TBinaryProtocol
    :members:

processor
---------

.. module:: aiothrift.processor

.. autoclass:: TProcessor
   :members:

server
------

.. currentmodule:: aiothrift.server

.. autoclass:: Server
   :members:

.. attribute:: make_server


