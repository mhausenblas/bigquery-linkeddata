import logging, cgi, os, platform, sys
import gflags as flags
import bigquery_db_api as bq
import bq_client

FLAGS = flags.FLAGS

def GetDefaultUsername():
  if platform.system() == 'Windows':
    homedir = os.path.join(os.environ['HOMEDRIVE'],
                           os.environ['HOMEPATH'])
  else:
    homedir = os.environ['HOME']
  return os.path.join(homedir, '.googlecookie')

flags.DEFINE_string('auth_file', 'michael.hausenblas@gmail.com',
                    'Path to authentication key.')
flags.DEFINE_string(
    'api_endpoint',
    'https://www.googleapis.com/bigquery/v1',
    'Bigquery REST/JSON-RPC endpoint.')
flags.DEFINE_enum('transport', 'REST', ['REST', 'RPC'],
                  'The transportation method')
flags.DEFINE_string('bigquery_db_api_rpclog', None,
                    'Log file for bigquery RPCs.')

class BigQueryWrapper:
	def execquery(self, qstr):
		logging.info("Executing query %s" %qstr)
		try:
			authpolicy = bq_client.CachedUserClientLoginAuthPolicy(FLAGS.auth_file)
			bqc = bq_client.BigQueryClient(authpolicy, FLAGS.transport, FLAGS.api_endpoint)
			return bqc.Query(qstr)
		except:
			#exctype, value = sys.exc_info()[:2]
			return "Can't execute query due to authentication issues ..."