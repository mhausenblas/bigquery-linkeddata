from google.appengine.ext import db
from google.appengine.ext.blobstore import blobstore

class BQueryModel(db.Model):
	author = db.UserProperty()
	querystr = db.TextProperty()
	qdesc = db.TextProperty()
	date = db.DateTimeProperty(auto_now_add=True)
	
class NTriplesFileModel(db.Model):
	fname = db.StringProperty()
	ntriplefile = blobstore.BlobReferenceProperty()
	date = db.DateTimeProperty(auto_now_add=True)
