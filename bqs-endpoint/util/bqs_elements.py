import logging, cgi, os, platform, sys, boto

from google.appengine.api import users

from util.bqs_global import *
from util.bqs_access import *

class BQSMenu:
	def menu_content(self, menuitem):
		if menuitem == 'Query': 
			content= '<span class="misel"><a href="/">Query</a></span> | <span><a href="/datasets">Data</a></span> | <span><a href="/about">About</a></span>' 
		elif menuitem == 'Data': 
			content= '<span><a href="/">Query</a><span> | <span class="misel"><a href="/datasets">Data</a></span> | <span><a href="/about">About</a></span>' 
		elif menuitem == 'About': 
			content= '<span><a href="/">Query</a><span> | <span><a href="/datasets">Data</a></span> | <span class="misel"><a href="/about">About</a></span>' 

		if users.is_current_user_admin():
			content = content + ' | <a href="/admin">Admin</a>'
		return content

	def user_content(self, request):
		currentuser = UserUtility()
		url, url_title, url_linktext = currentuser.usercredentials(request)
		usr = currentuser.renderuser(request)
		content = '<span class="signio"><a href="%s" title="%s">%s</a></span> %s' %(url, url_title, url_linktext, usr)
		return content
		
class BQSFooter:
	def footer_content(self):
		return """
			<p>
				See <a href="http://code.google.com/p/bigquery-linkeddata/">BigQuery For Linked Open Data</a> on Google Code for the source code.
				Kudos to FamFamFam's <a href="http://www.famfamfam.com/lab/icons/silk/">Silk icons</a> that are used under the CC Attribution 2.5 Generic license.
				<a href="http://code.google.com/appengine/">
			</p>
			<div style='text-algin: center'><img src="http://code.google.com/appengine/images/appengine-silver-120x30.gif" alt="Powered by Google App Engine" /></a></div>
			
		"""