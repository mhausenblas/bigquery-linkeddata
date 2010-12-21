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
		qstr = qstr.split(';')[0]
		logging.info("Trying to executing query <%s>" %qstr)
		try:
			authpolicy = bq_client.ClientLoginAuthPolicy()
			authpolicy.RetreiveToken('xxx@gmail.com', 'TOPSECRETPASSWORD') # you need to put your BigQuery account credentials here
			bqc = bq_client.BigQueryClient(authpolicy, FLAGS.transport, FLAGS.api_endpoint)
			(schema, qresult) = bqc.Query(qstr)
			logging.info("schema %s" %schema)
			logging.info("qresult %s" %qresult)
			return qresult
		except bq.DatabaseError, dbe:
			logging.info("%s" %dbe)
			return dbe # this means the provided GMail account has not been activated for BigQuery
		except urllib2.HTTPError, httpe:
			#logging.info("%s" %httpe)
			return "access forbidden" # this means you haven't provided the correct credentials
