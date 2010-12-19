from google.appengine.ext import db

class BQueryModel(db.Model):
	author = db.UserProperty()
	querystr = db.TextProperty()
	date = db.DateTimeProperty(auto_now_add=True)