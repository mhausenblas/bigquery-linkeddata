#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DB API 2.0 interface to bigquery tables.

This module defines a DB API 2.0 compliant interface to the bigquery
analysis system with standard DQL support.
"""

# This file is not style compliant because it must conform to the API spec.



import datetime
import exceptions
# In Python 2.6, the third-party simplejson became the bundled json
# module; so we look for the bundled one, and grab the third-party one
# if it's not available.
try:
  import json
except ImportError:
  import simplejson as json
import urllib
import urllib2


_DecodeJSON = json.JSONDecoder().decode
_EncodeJSON = json.JSONEncoder().encode

apilevel = '2.0'
threadsafety = 2
paramstyle = 'pyformat'

# Type objects.
STRING = 1
BINARY = 2
NUMBER = 3
BOOLEAN = 4
DATETIME = -1
ROWID = -1


def Date(y, m, d):
  raise NotSupportedError


def Time(h, m, s):
  raise NotSupportedError


def Timestamp(y, mon, d, h, m, s):
  raise NotSupportedError


def DateFromTicks(ticks):
  raise NotSupportedError


def TimeFromTicks(ticks):
  raise NotSupportedError


def TimestampFromTicks(ticks):
  raise NotSupportedError


def Binary(string):
  return string


# Bigquery API supports multiple transport options.
# These classes implement support for specific transport types.


class _ApiRequest(urllib2.Request):
  def __init__(self, method, url, data=None):
    urllib2.Request.__init__(self, url, data)
    self._method = method
    self.add_header('Content-Type', 'application/json')
    self.add_header('Accept', 'application/json')

  def get_method(self):
    return self._method


class _Transport(object):
  """Interface definition for a transport driver."""

  def prepare_request(self, base_url, request):
    """Build the appropriate http request."""
    raise NotImplementedError

  def process_response(self, body):
    """Process the response and extract data object."""
    raise NotImplementedError


class _RESTTransport(_Transport):
  GET = 'get'
  LIST = 'list'
  INSERT = 'insert'
  DELETE = 'delete'

  _HTTP_METHOD_MAP = {
      GET: 'GET',
      LIST: 'GET',
      INSERT: 'POST',
      DELETE: 'DELETE'
      }

  def prepare_request(self, base_url, request):
    url = base_url
    if 'parents' in request:
      url = url + '/' + '/'.join([
          '%s/%s' % (p[0], urllib.quote(p[1], safe=';'))
          for p in request['parents']])
    url = url + '/' + request['collection']
    operation = request.get('operation', None)
    body = None
    if (operation == _RESTTransport.GET or
        operation == _RESTTransport.DELETE):
      url += '/' + urllib.quote(request['resource_name'], safe=';')
    elif operation == _RESTTransport.INSERT:
      body = '{}'
    elif operation == _RESTTransport.LIST:
      pass
    elif operation:
      raise InterfaceError('Unknown REST operation: ' +
                           str(operation))
    params = request.get('params', None)
    if params:
      if body:
        # This is going to be a POST request.
        body = _EncodeJSON(dict(data=params))
      else:
        args = ['%s=%s' % (k, urllib.quote(v, safe=';'))
                for k, v in params.iteritems()]
        url += '?' + '&'.join(args)
    return _ApiRequest(self._HTTP_METHOD_MAP.get(operation, 'GET'),
                       url, body)

  def process_response(self, body):
    error, data = None, None
    if body:
      try:
        response = _DecodeJSON(str(body))
        error = response.get('error', None)
        if not error:
          data = response.get('data') or response
      except Exception, e:
        raise InterfaceError('Could not parse response: %s' % str(e))

    return (error, data)


class _RPCTransport(_Transport):
  def prepare_request(self, base_url, request):
    rpc = dict(id=request['id'],
               method=request['method'])
    params = request.get('params', {}).copy()
    params.update([(name, request[param])
                   for (param, name) in request.get('rpc_param_map', [])])
    rpc['params'] = params
    try:
      json_request = _EncodeJSON(rpc)
    except Exception, e:
      raise InterfaceError('Could not encode params: %s' % str(e))
    return _ApiRequest('POST', base_url, json_request)

  def process_response(self, body):
    response = None
    if body:
      try:
        response = _DecodeJSON(str(body))
      except Exception, e:
        raise InterfaceError('Could not parse response: %s' % str(e))

    data = None
    error = response.get('error', None)
    if not error:
      data = response.get('result') or response
    return (error, data)


_UNKNOWN_TYPE = ('UNKNOWN', -1, str)
_TYPE_MAP = {
    'float': ('FLOAT', NUMBER, float),
    'integer': ('INTEGER', NUMBER, int),
    'boolean': ('BOOLEAN', BOOLEAN, bool),
    'string': ('STRING', STRING, lambda v: v.encode('unicode_escape')),
    # ALPHA types still in use:
    'double': ('DOUBLE', NUMBER, float),
    'int64': ('INT64', NUMBER, long),
    'uint64': ('UINT64', NUMBER, long),
    'int32': ('INT32', NUMBER, int),
    'uint64': ('UINT64', NUMBER, long),
    'uint32': ('UINT32', NUMBER, long),
    'bool': ('BOOL', STRING, bool),
    'decimal': ('DECIMAL', NUMBER, float),
    'text': ('TEXT', STRING, lambda v: v.encode('unicode_escape')),
}

_rpc_log = None


def init_rpc_log(logfile):
  global _rpc_log
  _rpc_log = open(logfile, 'a')


def _log_rpc_info(*lines):
  if _rpc_log:
    prefix = str(datetime.datetime.now())
    _rpc_log.write(prefix)
    _rpc_log.write(' | ')
    prefix = (' ' * len(prefix)) + ' | '
    for i in xrange(len(lines)):
      if lines[i]:
        if i > 0: _rpc_log.write(prefix)
        _rpc_log.write(lines[i].rstrip().encode('string_escape'))
        _rpc_log.write('\n')
    _rpc_log.flush()


def _MapTypeToName(declared_type_code):
  return _TYPE_MAP.get(
      declared_type_code, _UNKNOWN_TYPE)[0]


class _BigqueryResult(object):
  """The result of a bigquery DQL statement."""

  def __init__(self, stmt, results):
    self._statement = stmt
    self._description = [
        (f['id'], _TYPE_MAP.get(f.get('type'), _UNKNOWN_TYPE)[1],
         None, None, None, None, True)
        for f in results['fields']]
    extractors = [
        _TYPE_MAP.get(f.get('type'), _UNKNOWN_TYPE)
        for f in results['fields']]
    num_columns = len(extractors)
    if 'rows' in results:
      rows = map(lambda x: x['f'], results['rows'])
    else:
      rows = []
    self._rowcount = len(rows)
    self._rows = []

    def transform(field, type_info):
      val = field['v']
      if val is not None:
        val = type_info[2](val)
      return val

    for row in rows:
      if len(row) != num_columns:
        raise DataError('Malformed rows')
      self._rows.append(tuple([
          transform(row[i], extractors[i])
          for i in xrange(num_columns)]))

  def description(self):
    return self._description

  def rowcount(self):
    return self._rowcount

  def remaining(self):
    return len(self._rows)

  def fetch(self, count):
    num = self.remaining()
    count = min(count, num)
    rows = self._rows[0:count]
    del self._rows[0:count]
    return rows


class _BigqueryCursor(object):
  """Bigquery cursor object implementation."""

  def __init__(self, connection):
    """Creates a cursor attached to the given connection."""

    self._conn = connection
    self._result = None
    self.arraysize = 1

  def __get_description(self):
    if not self._conn:
      raise ProgrammingError
    if self._result:
      return self._result.description()
    else:
      return None
  description = property(__get_description)

  def __get_rowcount(self):
    if not self._conn:
      raise ProgrammingError
    if self._result:
      return self._result.num_results()
    else:
      return -1
  rowcount = property(__get_rowcount)

  def callproc(self, proc, *params):
    """Not supported."""
    if not self._conn:
      raise ProgrammingError
    raise NotSupportedError

  def close(self):
    """Closes the cursor."""
    if not self._conn:
      raise ProgrammingError
    self._conn = None
    del self._result

  def execute(self, statement_tpl, **params):
    """Execute the statement with the given params."""
    if not self._conn:
      raise ProgrammingError
    statement = (statement_tpl % params) + ';'
    if self._result:
      del self._result
    result = self._conn.Call(
        dict(method='bigquery.query',
             collection='query',
             params=dict(q=statement)))
    # Catch all unpacking errors.
    try:
      self._result = _BigqueryResult(statement, result)
    except Exception, e:
      raise DataError('Could not unpack results: %s' % str(e))

  def executemany(self, operation, param_sets):
    """Not supported."""
    if not self._conn:
      raise ProgrammingError
    raise NotSupportedError

  def setinputsizes(self, sizes):
    """Implementation ignores this hint."""
    pass

  def setoutputsize(self, size, column=None):
    """Implementation ignores this hint."""
    pass

  def nextset(self):
    """Advances the cursor to the next result set, if any."""
    if not self._conn:
      raise ProgrammingError
    if self._result:
      del self._result
      self._result = None
    return None

  def fetchone(self):
    """Fetch one row of the results."""
    if not self._conn or not self._result:
      raise ProgrammingError
    rows = self._result.fetch(1)
    if not rows:
      return None
    return rows[0]

  def fetchmany(self, size=None):
    """Fetch upto size rows of the results."""
    if not self._conn or not self._result:
      raise ProgrammingError
    if not size:
      size = self.arraysize
    return self._result.fetch(size)

  def fetchall(self):
    """Fetch all remaining rows."""
    if not self._conn or not self._result:
      raise ProgrammingError
    return self._result.fetch(self._result.remaining())

  def _unpack_columns(self, fields, prefix=''):
    columns = []
    for f in fields:
      if f['type'] == 'record':
        columns.extend(self._unpack_columns(
            f['fields'], prefix + f['id'] + '.'))
      else:
        columns.append(
            (prefix + f['id'], _MapTypeToName(f['type']),
             None, None, None, True))
    return columns

  def bq_get_table_metadata(self, table):
    """Fetch the metadata for a table."""
    result = self._conn.Call(
        dict(method='bigquery.tables.get',
             collection='tables',
             operation=_RESTTransport.GET,
             resource_name=table,
             rpc_param_map=[('resource_name', 'name')]))

    # Catch all result unpacking errors.
    try:
      columns = self._unpack_columns(result['fields'])
    except Exception, e:
      raise DataError('Metadata fetch failed: %s' % str(e))
    return columns


class _BigqueryConnection(object):
  """Connection to the Bigquery API."""

  def __init__(self, endpoint, auth, transport):
    """Initializes a connection to bigquery."""
    self._endpoint = endpoint
    self._auth = auth
    self._transport = transport
    self._request_id = 1000

  def cursor(self):
    """Returns a cursor attached to this connection."""
    return _BigqueryCursor(self)

  def commit(self):
    """Noop on bigquery."""
    pass

  def rollback(self):
    """Noop on bigquery."""
    pass

  def close(self):
    """Noop on bigquery."""
    pass

  def Call(self, request):
    """Issue the HTTP request to execute an API method."""
    request['id'] = self._request_id
    self._request_id += 1

    http_req = self._transport.prepare_request(self._endpoint, request)

    request_log = [http_req.get_method() + ' ' + http_req.get_full_url()]
    if http_req.has_data():
      request_log += ['>>>>', http_req.get_data()]
    _log_rpc_info(*request_log)
    http_req.add_header('Authorization', 'GoogleLogin auth=' + self._auth)
    try:
      http_response = urllib2.urlopen(http_req)
    except urllib2.HTTPError, e:
      http_response = e
    except Error, e:
      _log_rpc_info('Failed: %s\n' % str(e), '----')
      raise OperationalError('RPC failed: %s' % str(e))

    body = http_response.read()
    _log_rpc_info('<<<< [%d]' % http_response.code, body, '----')

    (error, data) = self._transport.process_response(body)

    if error:
      message = error.get('message', 'Unknown database error')
      raise DatabaseError('Database error: %s' % message)

    return data


# Transport options.
REST = _RESTTransport()
RPC = _RPCTransport()


def connect(host, auth, transport=REST):
  """Connect to the given bigquery API host with the provided auth.

  Args:
    host: hostname of the api server
    auth: string of file-like object containing authentication data
    transport: Underlying tranport used by driver (REST, RPC).

  Returns:
    Bigquery DB connection

  Raises:
    ProgrammingError: if either host or auth is invalid
  """
  if not host or not auth:
    raise ProgrammingError
  return _BigqueryConnection(host, auth, transport)


class Error(exceptions.StandardError):
  pass


class Warning(exceptions.StandardError):
  pass


class InterfaceError(Error):
  pass


class DatabaseError(Error):
  pass


class InternalError(DatabaseError):
  pass


class OperationalError(DatabaseError):
  pass


class ProgrammingError(DatabaseError):
  pass


class IntegrityError(DatabaseError):
  pass


class DataError(DatabaseError):
  pass


class NotSupportedError(DatabaseError):
  pass
