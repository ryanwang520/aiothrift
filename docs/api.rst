.. _api:

API
===

.. module:: aiothrift

This part of the documentation covers all the interfaces of aiothrift.  For
parts where aiothrift depends on external libraries, we document the most
important right here and provide links to the canonical documentation.


ThriftConnection Object
-----------------------


.. autoclass:: ThriftConnection
   :members:
   :private-members:

ThriftConnection Pool
---------------------

.. autoclass:: ThriftPool
   :members:
   :inherited-members:

protocol
--------

.. autoclass:: TProtocol
    :members:

.. autoclass:: TBinaryProtocol
    :members:
    :inherited-members:

processor
---------

.. autoclass:: TProcessor
   :members:
   :inherited-members:

server
------

.. autoclass:: Server
   :members:
   :inherited-members:

exceptions
----------

.. autoclass:: ThriftError
    :members:

.. autoclass:: ConnectionClosedError
    :members:

.. autoclass:: PoolClosedError
    :members:

.. autoclass:: ThriftAppError
    :members:


Useful functions
----------------

.. autofunction:: create_server

.. autofunction:: create_connection

.. autofunction:: create_pool

