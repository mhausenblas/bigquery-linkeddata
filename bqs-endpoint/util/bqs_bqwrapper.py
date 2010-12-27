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
	
	def import_table(self, source_file):
		infile = "/".join([GlobalUtility.IMPORT_BUCKET, GlobalUtility.IMPORT_OBJECT, source_file])
		rdftable = "/".join([GlobalUtility.IMPORT_BUCKET, GlobalUtility.RDFTABLE_OBJECT])
		logging.info("Importing %s into table %s" %(infile, rdftable))
		bqc = bq_client.BigQueryClient(self.authq(), FLAGS.transport, FLAGS.api_endpoint)
		result = bqc.StartImport(rdftable, infile)
		# result something like ... {u'table': u'lodcloud/rdftable', u'kind': u'bigquery#import_id', u'import': u'81bbae7030fa680d'}
		logging.info("Import started with: %s" %result)
		return result['import']
	
	def get_import_status(self, importid):
		rdftable = "/".join([GlobalUtility.IMPORT_BUCKET, GlobalUtility.RDFTABLE_OBJECT])
		bqc = bq_client.BigQueryClient(self.authq(), FLAGS.transport, FLAGS.api_endpoint)
		result = bqc.GetImportStatus(rdftable, importid)
		#logging.info("Import status of %s is: %s" %(importid, result))
		return result['status']
		
	def execquery(self, qstr):
		qstr = qstr.replace('\\n',' ')
		qstr = qstr.rstrip()
		if ';' in qstr:	
			qstr = qstr.split(';')[0]
		logging.info("Trying to execute query <%s>" %qstr)
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
			return 0

	def authq(self):
		authpolicy = bq_client.ClientLoginAuthPolicy()
		authpolicy.RetreiveToken(GlobalUtility.GS_ENABLED_GMAIL_ADDRESS, GlobalUtility.GS_ENABLED_GMAIL_PWD)
		return authpolicy