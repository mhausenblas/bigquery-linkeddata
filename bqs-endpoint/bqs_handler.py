import logging, cgi, os, platform, sys, urllib, boto

from google.appengine.ext import webapp
from google.appengine.api import users
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template

from util.bqs_access import *
from util.bqs_global import *
from util.bqs_bqwrapper import *
#from util.nt2csv import *

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
			'isadmin' : users.is_current_user_admin()
		}
		path = os.path.join(os.path.dirname(__file__), 'index.html')
		self.response.out.write(template.render(path, templatev))
		
	def post(self):
		logging.info("Saving query ...")
		bqm = BQueryModel()
		if users.get_current_user():
			bqm.author = users.get_current_user()
		bqm.querystr = self.request.get('querystr')
		qkey = bqm.put()

class ExecQueryHandler(webapp.RequestHandler):
	def get(self):
		bqw = BigQueryWrapper()
		qstr = self.request.get('querystr')
		results = bqw.execquery(qstr)
		self.response.out.write(results)

class ImportHandler(webapp.RequestHandler):
	def get(self):
		upload_url = blobstore.create_upload_url('/upload')
		datasetlinks = self.listds()
		uploadlinks = self.listuploads()
		currentuser = UserUtility()
		url, url_linktext = currentuser.usercredentials(self.request)
		templatev = {
			'upload_url': upload_url,
			'datasetlinks': datasetlinks,
			'uploadlinks': uploadlinks,
			'login_url': url,
			'login_url_linktext': url_linktext,
			'help_url' : GlobalUtility.HELP_LINK,
			'isadmin' : users.is_current_user_admin()
		}
		path = os.path.join(os.path.dirname(__file__), 'import.html')
		self.response.out.write(template.render(path, templatev))

	def listuploads(self):
		uploadfquery = NTriplesFileModel.all().order('fname')
		#logging.info("Available files:")
		#for uploadf in uploadfquery:
		#	logging.info(uploadf.fname)
		return uploadfquery
		
	def listds(self):
		self.gsInit()
		uri = boto.storage_uri(GlobalUtility.IMPORT_BUCKET, "gs")
		buckets = uri.get_bucket()
		return [self.gsLink(bobject) for bobject in buckets]

	def gsInit(self):
		config = boto.config
		try:
			config.add_section('Credentials')
			config.set('Credentials', 'gs_access_key_id', 'GOOGRFRRGDV2QKUSC5E4')
			config.set('Credentials', 'gs_secret_access_key', 'yMkkshmk5+i2o3ryCtmLxfuyVBLidiu1WDj1GXux')
		except:
			pass

	def gsLink(self, bobject):
		if not bobject.name.find(GlobalUtility.METADATA_POSTFIX) >= 0: # only process non-metadata files
			absbucketURI = "/".join([GlobalUtility.GOOGLE_STORAGE_BASE_URI, GlobalUtility.IMPORT_BUCKET, bobject.name])
			dataset_name = bobject.name.split("/")[1].split(".")[0] # abc/xxx.nt -> xxx
			graph_uri = self.gsGetMetadata(bobject, 'graphURI')
			logging.info("Got %s from graph %s in Google Storage" %(bobject.name, graph_uri))
			bobjectlink = '<td>%s</td><td><a href="%s" target="_new">gs://%s</a></td><td>%s</td>' %(dataset_name, absbucketURI, "/".join([GlobalUtility.IMPORT_BUCKET, bobject.name]), graph_uri)
			return bobjectlink
		else:
			return ""

	def gsGetMetadata(self, bobject, m_key):
		gsm_target = "/".join([GlobalUtility.IMPORT_BUCKET, bobject.name.split(".")[0]])
		gsm_target = ".".join([gsm_target, GlobalUtility.METADATA_POSTFIX])
		logging.info("Looking for metadata at %s " %gsm_target)
		try:
			gskey = boto.storage_uri(gsm_target, "gs")
			ometadata = gskey.get_contents_as_string()
			graph_uri = ometadata.split("=")[1]
			logging.info("Got metadata %s in Google Storage" %ometadata)
		except:
			graph_uri = GlobalUtility.DEFAULT_GRAPH_URI
		return graph_uri

	def post(self):
		pass

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
	def post(self):
		uploads = self.get_uploads('ntfile')
		blob_info = uploads[0]
		# check if filename already exists and if not, upload it
		if not self.fileexists(blob_info.filename):
			logging.info("Imported %s (content type: %s, size: %s)" %(blob_info.filename, blob_info.content_type, blob_info.size))
			# save reference to file blob
			ntf = NTriplesFileModel()
			ntf.fname = blob_info.filename
			ntf.ntriplefile = blob_info.key()
			ntf.put()
			# convert NTriples to BigQuery CSV format
			graphURI = self.request.get('tgraphURI')
			logging.info("Graph URI: %s" %graphURI)
			ntfilecontent =  self.get_ntfile_contents(blob_info.key())
			#nt2csv = NTriple2CSV(None, graphURI)
			#csv_str = nt2csv.convert_str()
			csv_str = ntfilecontent
			# copy CSV file to Google storage
			self.gsInit()
			self.gsCopy(csv_str, blob_info.filename, graphURI)
			self.redirect('/datasets')
		else:
			logging.info("%s is already available here, not gonna upload it again ..." %blob_info.filename) 
			self.redirect('/datasets')

	def fileexists(self, fname):
		fileavailq = db.GqlQuery("SELECT * FROM NTriplesFileModel WHERE fname = :1", fname)
		if fileavailq.count(1) > 0:
			return True
		else:
			return False

	def get_ntfile_contents(self, blob_key):
		blob_reader = blobstore.BlobReader(blob_key, buffer_size=1048576) # set buffer to 1MB
		return blob_reader.read()
		
	def gsCopy(self, csv_str, fname, graph_uri):
		gs_target = "/".join([GlobalUtility.IMPORT_BUCKET, GlobalUtility.IMPORT_OBJECT, fname])
		uri = boto.storage_uri(gs_target, "gs")
		gskey = uri.new_key()
		gskey.set_contents_from_string(csv_str, None, True) # see http://boto.cloudhackers.com/ref/gs.html#boto.gs.key.Key.set_contents_from_filename
		gskey.set_acl('public-read')
		# this doesn't seem to work: gskey.set_metadata('graphURI', graphURI) ... hence a workaround:
		self.gsSetMetadata(fname, "graphURI=%s" %graph_uri)
		logging.info("Copied %s into graph %s in Google Storage" %(gs_target, graph_uri))
	
	def gsSetMetadata(self, fname, metadata):
		gs_target = "/".join([GlobalUtility.IMPORT_BUCKET, GlobalUtility.IMPORT_OBJECT, fname.split(".")[0]])
		gs_target = ".".join([gs_target, GlobalUtility.METADATA_POSTFIX])
		uri = boto.storage_uri(gs_target, "gs")
		gskey = uri.new_key()
		gskey.set_contents_from_string(metadata, None, True) # see http://boto.cloudhackers.com/ref/gs.html#boto.gs.key.Key.set_contents_from_filename
	
		
	def gsInit(self):
		config = boto.config
		try:
			config.add_section('Credentials')
			config.set('Credentials', 'gs_access_key_id', 'GOOGRFRRGDV2QKUSC5E4')
			config.set('Credentials', 'gs_secret_access_key', 'yMkkshmk5+i2o3ryCtmLxfuyVBLidiu1WDj1GXux')
		except:
			pass

class AdminBQSEndpointHandler(webapp.RequestHandler):
		def get(self):
			user = users.get_current_user()
			if user:
				if users.is_current_user_admin():
					self.response.out.write("<div><a href=\"/\">Home</a> | <a href=\"/admin?cmd=remove_uploads\">Remove uploaded files</a> | <a href=\"/admin?cmd=reset_datasets\">Remove all datasets</a></div>")
					if self.request.get('cmd'):
						logging.info("Trying to execute admin command: %s" %self.request.get('cmd'))
						self.dispatchcmd(self.request.get('cmd'))
				else:
					self.response.out.write("Hi %s - you're not an admin, right? ;)" % user.nickname())

		def dispatchcmd(self, value):
			mname = 'exec_' + str(value)
			try:
				methodcall = getattr(self, mname)
				methodcall()
			except:
				self.response.out.write("<pre style='color:red'>Command unknown</pre>")
		
		def exec_remove_uploads(self):
			# remove all blobs
			allblobs = blobstore.BlobInfo.all();
			more = (allblobs.count() > 0)
			blobstore.delete(allblobs)
			# remove all references to blobs
			ntfiles = NTriplesFileModel.all()
			for ntfile in ntfiles:
			    ntfile.delete()
			self.response.out.write("<pre style='color:red'>Removed all uploaded files.</pre>")
		
		def exec_reset_datasets(self):
			# remove all objects from IMPORT_BUCKET
			self.gsInit()
			uri = boto.storage_uri(GlobalUtility.IMPORT_BUCKET, "gs")
			bucket = uri.get_bucket()
			for bobject in bucket:
				curkey = "/".join([GlobalUtility.IMPORT_BUCKET, bobject.name])
				try:
					gskey = boto.storage_uri(curkey, "gs")
					ometadata = gskey.delete_key()
					self.response.out.write("<p>Removed: %s</p>" %bobject.name)
				except:
					self.response.out.write("<p>Unable to remove: %s</p>" %bobject.name)
			self.response.out.write("<pre style='color:red'>Removed all datasets from Google Storage.</pre>")
			
		def gsInit(self):
			config = boto.config
			try:
				config.add_section('Credentials')
				config.set('Credentials', 'gs_access_key_id', 'GOOGRFRRGDV2QKUSC5E4')
				config.set('Credentials', 'gs_secret_access_key', 'yMkkshmk5+i2o3ryCtmLxfuyVBLidiu1WDj1GXux')
			except:
				pass