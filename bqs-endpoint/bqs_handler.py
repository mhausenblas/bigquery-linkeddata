import logging, cgi, os, platform, sys, boto

from google.appengine.ext.webapp import template
from google.appengine.ext import webapp
from google.appengine.api import users

from util.bqs_access import *
from util.bqs_global import *
from util.bqs_bqwrapper import *

from bqs_models import *

class OverviewHandler(webapp.RequestHandler):
	def get(self):
		bqquery = BQueryModel.all().order('-date')
		squeries = bqquery.fetch(10)
		currentuser = UserUtility()
		url, url_linktext = currentuser.usercredentials(self.request)
		templatev = {
			'bqueries': squeries,
			'login_url': url,
			'login_url_linktext': url_linktext,
			'help_url' : GlobalUtility.HELP_LINK,
		}
		path = os.path.join(os.path.dirname(__file__), 'index.html')
		self.response.out.write(template.render(path, templatev))
		
	def post(self):
		logging.info("Safing query ...")
		bqm = BQueryModel()
		if users.get_current_user():
			bqm.author = users.get_current_user()
		bqm.querystr = self.request.get('querystr')
		qkey = bqm.put()
		#self.get()
		
class ExecQueryHandler(webapp.RequestHandler):
	def get(self):
		bqw = BigQueryWrapper()
		qstr = self.request.get('querystr')
		(meta, results) = bqw.execquery(qstr)
		self.response.out.write(results)
		#self.redirect('/')

class ImportHandler(webapp.RequestHandler):
	def get(self):
		currentuser = UserUtility()
		url, url_linktext = currentuser.usercredentials(self.request)
		datasetlinks = self.listds()
		templatev = {
			'datasetlinks': datasetlinks,
			'login_url': url,
			'login_url_linktext': url_linktext,
			'help_url' : GlobalUtility.HELP_LINK,
		}
		path = os.path.join(os.path.dirname(__file__), 'import.html')
		self.response.out.write(template.render(path, templatev))

	def listds(self):
		self.gsInit()
		uri = boto.storage_uri(GlobalUtility.IMPORT_BUCKET, "gs")
		buckets = uri.get_bucket()
		return [self.gsLink(bucket) for bucket in buckets]

	def gsInit(self):
		config = boto.config
		try:
			config.add_section('Credentials')
			config.set('Credentials', 'gs_access_key_id', 'GOOGRFRRGDV2QKUSC5E4')
			config.set('Credentials', 'gs_secret_access_key', 'yMkkshmk5+i2o3ryCtmLxfuyVBLidiu1WDj1GXux')
		except:
			pass

	def gsLink(self, bucket):
		absbucketURI = "/".join([GlobalUtility.GOOGLE_STORAGE_BASE_URI, GlobalUtility.IMPORT_BUCKET, bucket.name])
		bucketlink = '<a href="%s" target="_new">%s</a>' %(absbucketURI, bucket.name)
		return bucketlink

	def post(self):
		pass