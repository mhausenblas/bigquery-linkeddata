import logging, cgi, os, platform, sys, urllib

from google.appengine.api import users

class QueryHelper:
	def render_query(self, squery):
		if users.is_current_user_admin():
			qctrl = '<img src="/img/execute.png" class="execqsaved" alt="execute" title="execute query" /> <img src="/img/delete.png" class="deleteqsaved" alt="delete" title="delete query" />'
		else:
			qctrl = '<img src="/img/execute.png" class="execqsaved" />'
		query = '<div class="sq">%s</div>' %cgi.escape(squery.querystr)
		if squery.author:
			author = '<div class="byuser">by <em>%s</em></div>' %squery.author.nickname()
		else:
			author = '<div class="byuser">by anonymous</div>'
		return "".join(['<div class="savedquery" id="%s">' %squery.key(), qctrl, query, author, '</div>'])
