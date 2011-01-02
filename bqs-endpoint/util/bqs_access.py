import logging, cgi, os, platform, sys, boto

from google.appengine.api import users

from util.bqs_global import *

class UserUtility:
	def usercredentials(self, request):
		if users.get_current_user():
			url = users.create_logout_url(request.uri)
			url_title = 'Sign out ...'
			url_linktext = '<img src="/img/online.png" alt="logged in" style="vertical-align: middle" />'
		else:
			url = users.create_login_url(request.uri)
			url_title = 'Sign in ...'
			url_linktext = '<img src="/img/offline.png" alt="not logged in" style="vertical-align: middle" />'
		return url, url_title, url_linktext
		
	def renderuser(self, request):
		if users.get_current_user():
			return users.get_current_user().email()
		else:
			return "Anonymous"
			
class GSHelper:
	def gs_init(self):
		config = boto.config
		try:
			config.add_section('Credentials')
			config.set('Credentials', 'gs_access_key_id', GlobalUtility.GS_ACCESS_KEY_ID)
			config.set('Credentials', 'gs_secret_access_key', GlobalUtility.GS_SECRECT_ACCESS_KEY)
		except:
			pass