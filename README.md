# PyPresto

PyPresto is a client protocol implementation for [prestodb](http://prestodb.io).
Presto is a distributed SQL query engine for big data.

This client implements asynchronous calls and does basic provisioning for resultsets.

Sorry for the lacking documentation, I'd try to make a more comprehensive version later.

### Requirements
* Python 2.7 or higher


### Installation
```
    pip install pypresto (DID NOT PUT pip YET)
```
or, execute from the source directory
```
python setup.py install
```

### Client() options
```
  hostnames     A list of optional hostnames to connect to, currently a random hostname is used from the list per query
  port          Port to connect to (default: 8080)
  user          User name (default: 'nobody')
```
### client.connect options
```
  max_workers   Maximum number of workrs to spawn when we're running queries asynchronously (default:6)
  catalog       Catalog name (default: 'default')
  schema        Schema name (default: 'hive')
  result_mode   How to would results be returned, the two options are either 'dict' or 'list' (default: 'dict')

```

### Usage examples

Simple querying:

```python
from pypresto import Client

client = Client(['127.0.0.1'])
session = client.connect(catalog='cassandra', schema="myschema")
q = session.query('SELECT * FROM mytable')
for row in q.iter_results():
    print('%r' % row)
```

Using futures:

```python
from pypresto import Client

futures = []
for i in range(10):
    futures.append(session.query_async('SELECT * FROM mytable where my_int=%d', [i]))

for future in futures:
    for row in future.result().iter_results():
      print('%r' % row)
```

### License

[The MIT License (MIT)](http://opensource.org/licenses/MIT)
