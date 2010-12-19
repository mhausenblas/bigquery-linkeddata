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

"""A client for BigQuery api to simplify access.

BigQueryClient() exposes table management, import request, and query methods.
It must be initialized with a ClientLoginAuthPolicy to provide authentication
credentials. Two are provided here: KeystoreClientLoginAuthPolicy() and
CachedUserClientLoginAuthPolicy().
"""



import getpass
try:
  # pylint: disable-msg=C6204
  import json
except ImportError:
  # pylint: disable-msg=C6204
  import simplejson as json
# pylint: disable-msg=C6204
import os
import sys
import urllib
import urllib2


import bigquery_db_api as bq


# Constants used for ClientLogin negotiation.
_SERVICE = 'ndev'
_SOURCE = 'bq_client'


class SchemaError(Exception):
  """Schema error."""


class ClientLoginAuthPolicy(object):
  """Base ClientLoginAuthPolicy providing common functionality."""

  def __init__(self):
    self.auth_token_ = None

  def RetreiveToken(self, email, password):
    """Retreives ClientLogin token from server."""
    data = urllib.urlencode(dict(
        service=_SERVICE,
        source=_SOURCE,
        accountType='HOSTED_OR_GOOGLE',
        Email=email,
        Passwd=password))
    response = urllib2.urlopen(
        'https://www.google.com/accounts/ClientLogin', data)
    self.auth_token_ = None
    result = []
    for line in response:
      result.append(line)
      if line.startswith('Auth='):
        self.auth_token_ = line[len('Auth='):].strip()
        break
    if not self.auth_token_:
      raise RuntimeError('Could not login: ' + ''.join(result))

  def GetToken(self):
    """Returns the ClientLogin auth token for the configured user.

    Note: This is the only method called by BigQueryClient.

    Returns:
      ClientLogin authentication token.
    Raises:
      RuntimeError: If no authentication token is available.
    """
    if self.auth_token_:
      return self.auth_token_
    raise RuntimeError('ClientLoginAuthPolicy is not logged in.')




class CachedUserClientLoginAuthPolicy(ClientLoginAuthPolicy):
  """A ClientLoginAuthPolicy that retreives a cached token from a file."""

  def __init__(self, auth_cache_filename):
    ClientLoginAuthPolicy.__init__(self)
    self.auth_cache_filename_ = auth_cache_filename
    try:
      fp = open(auth_cache_filename)
      try:
        self.auth_token_ = fp.read()
      finally:
        fp.close()
    except IOError:
      # Lack of a file is not a fatal error, caller may call Login().
      pass

  def Login(self, email):
    """Prompts user for password and updates cache with new token."""
    if not sys.stdin.isatty():
      raise RuntimeError('Requires password entry.')
    print 'Logging in as', email
    password = getpass.getpass()
    self.RetreiveToken(email, password)

    # Update cached token.
    old_umask = os.umask(077)
    try:
      cf = open(self.auth_cache_filename_, 'w')
      try:
        cf.write(self.GetToken())
      finally:
        cf.close()
    finally:
      os.umask(old_umask)


class BigQueryClient(object):
  """Exposes bigquery REST/RPC api calls as synchronous method calls."""

  def __init__(self, auth_policy, transport, api_endpoint):
    """Initializes BigQueryClient.

    Args:
      auth_policy: An implementation of ClientLoginAuthPolicyInterface.
      transport: 'RPC' or 'REST'. Determines transport protocol to use to
        communicate with BigQuery service.
      api_endpoint: Endpoint to use to talk with BigQuery service. Ex:
        "http://www.googleapis.com/bigquery/v1".
    Raises:
      ValueError: On invalid transport argument.
    """
    self.auth_policy_ = auth_policy
    if transport == 'RPC':
      self.transport_ = bq.RPC
    elif transport == 'REST':
      self.transport_ = bq.REST
    else:
      raise ValueError('Invalid transport "%s"' % str(transport))
    self.api_endpoint_ = api_endpoint

  def _Connect(self):
    """Returns a configured bigquery_db_api connection."""
    return bq.connect(self.api_endpoint_,
                      self.auth_policy_.GetToken(),
                      self.transport_)

  def CreateTableFromFile(self, table_name, schema_path):
    """Creates a bigquery table, reading the schema from a local file.

    Args:
      table_name: Bigstore reference to the table to create. Authenticated
        user must have write access to the containing bucket.
      schema_path: Path to file containing the JSON schema description.
    Returns:
      Dictionary containing result of CreateTable request.
    Raises:
      SchemaError: if the schema_path does not exist or is invalid.
      bigquery_db_api.OperationalError: on connection failure.
      bigquery_db_api.DatabaseError: if the Create Table call failed.
    """
    try:
      schema_file = open(schema_path)
      schema_json = schema_file.read()
      schema_file.close()
    except IOError, e:
      raise SchemaError('Could not read file (%s):\n%s' %
                        (schema_path, str(e)))
    return self.CreateTableFromJson(table_name, schema_json)

  def CreateTableFromJson(self, table_name, schema_json):
    """Creates a bigquery table using the provided schema definition.

    Args:
      table_name: Bigstore reference to the table to create. Authenticated
        user must have write access to the containing bucket.
      schema_json: String containing the JSON schema description.
    Returns:
      Dictionary containing result of CreateTable request.
    Raises:
      SchemaError: If the schema_json is invalid.
      bigquery_db_api.OperationalError: On connection failure.
      bigquery_db_api.DatabaseError: If the Create Table call failed.
    """
    try:
      schema = json.JSONDecoder().decode(schema_json)
    except ValueError, e:
      raise SchemaError('Could not parse fields:\n%s\n%s' %
                        (schema_json, str(e)))

    conn = self._Connect()
    result = conn.Call(
        dict(method='bigquery.tables.insert',
             collection='tables',
             operation=bq.REST.INSERT,
             params=dict(name=table_name, fields=schema)))
    return result

  def DeleteTable(self, table_name):
    """Deletes a bigquery table.

    Args:
      table_name: Bigstore reference to the table to delete. Authenticated
        user must have write access to table_name.
    Returns:
      Dictionary containing result of delete operation.
    """
    conn = self._Connect()
    result = conn.Call(
        dict(method='bigquery.tables.delete',
             collection='tables',
             operation=bq.REST.DELETE,
             resource_name=table_name,
             rpc_param_map=[('resource_name', 'name')]))
    return result

  def DescribeTable(self, table_name):
    """Get the schema for a bigquery table.

    Args:
      table_name: Bigstore reference to the table to describwe. Authenticated
        user must have read access to table_name.
    Returns:
      Dictionary containing schema description for table_name.
    """
    conn = self._Connect()
    cursor = conn.cursor()
    return cursor.bq_get_table_metadata(table_name)

  def Query(self, query):
    """Issue a Query.

    Args:
      query: Query string without semicolon. Authenticated user
        must have read access to tables referenced in query.
    Returns:
      Tuple of two dictionaries: the first is the schema description
      of the result, the second the query results.
    """
    conn = self._Connect()
    cursor = conn.cursor()
    cursor.execute(query)
    meta = cursor.description
    return (meta, cursor.fetchall())

  def GetImportStatus(self, table_name, import_id):
    """Get the status of an import to a table.

    Args:
      table_name: Bigstore reference to the table to create. Authenticated
        user must have write/owner access to table_name.
      import_id: Import Id of the import to table_name.
    Returns:
      Dictionary containing results of the import status check.
    """
    conn = self._Connect()
    result = conn.Call(
        dict(method='bigquery.imports.get',
             parents=[('tables', table_name)],
             collection='imports',
             operation=bq.REST.GET,
             resource_name=import_id,
             rpc_param_map=[('resource_name', 'import_id')]))
    return result

  def StartImport(self, table_name, uris):
    """Starts an import operation.

    Args:
      table_name: Bigstore reference to table name to import to. Authenticated
        user must have write/owner access to table_name.
      uris: A single bigstore_ref or a list of bigstore_refs to import.
        Authenticated user must have read access to all bigstore objects
        referenced.
    Returns:
      Dictionary containing results of import request.
    """
    if type(uris).__name__ == 'str':
      uris = [uris]

    conn = self._Connect()
    result = conn.Call(
        dict(method='bigquery.imports.insert',
             parents=[('tables', table_name)],
             collection='imports',
             operation=bq.REST.INSERT,
             params=dict(sources=[dict(uri=u) for u in uris])))
    return result
