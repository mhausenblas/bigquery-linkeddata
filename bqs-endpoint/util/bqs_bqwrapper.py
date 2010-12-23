import logging, cgi, os, platform, sys, re
import urllib2
import gflags as flags
import bigquery_db_api as bq
import bq_client

from util.bqs_global import *

FLAGS = flags.FLAGS

flags.DEFINE_enum('transport', 'REST', ['REST', 'RPC'],
                  'The transportation method')
flags.DEFINE_string(
    'api_endpoint',
    'https://www.googleapis.com/bigquery/v1',
    'Bigquery REST/JSON-RPC endpoint.')


class BigQueryWrapper:
	def execquery(self, qstr):
		qstr = re.sub(r'\n',' ', qstr)
		#qstr = qstr.split(';')[0]
		#logging.info("Trying to execute query <%s>" %qstr)	
		try:
			bqc = bq_client.BigQueryClient(self.authq(), FLAGS.transport, FLAGS.api_endpoint)
			(schema, qresult) = bqc.Query(qstr)
			#logging.info("schema %s" %schema)
			#logging.info("qresult %s" %qresult)
			return schema, qresult
		except bq.DatabaseError, dbe:
			logging.info("%s" %dbe)
			return (dbe , "") # this means a syntax error or the provided GMail account has not been activated for BigQuery yet
		except urllib2.HTTPError, httpe:
			#logging.info("%s" %httpe)
			return ("Access forbidden. Check Google Storage credentials ...", "") # this means you haven't provided the correct credentials
	
	def numtriples(self):
		try:
			bqc = bq_client.BigQueryClient(self.authq(), FLAGS.transport, FLAGS.api_endpoint)
			(schema, qresult) = bqc.Query(GlobalUtility.DEFAULT_QUERY_COUNT)
			return qresult[0]
		except:
			return "unknown"

	def authq(self):
		authpolicy = bq_client.ClientLoginAuthPolicy()
		authpolicy.RetreiveToken(GlobalUtility.GS_ENABLED_GMAIL_ADDRESS, GlobalUtility.GS_ENABLED_GMAIL_PWD)
		return authpolicy