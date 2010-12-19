"""
The main GAE script for the BQS Endpoint UI.

This tool is part of the U{BigQuery for Linked Data <http://code.google.com/p/bigquery-linkeddata/>} effort.

@author: Michael Hausenblas, http://sw-app.org/mic.xhtml#i
@since: 2010-12-17
@status: inital version
"""
import logging, cgi, os, platform, sys, boto

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from bqs_handler import *

application = webapp.WSGIApplication([
						('/saveq', OverviewHandler),
						('/execq', ExecQueryHandler),
						('/datasets', ImportHandler),
						('/.*', OverviewHandler),
					],
					debug=True)

def main():
    run_wsgi_app(application)

if __name__ == '__main__':
	main()
