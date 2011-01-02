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
from bqs_api_handler import *

application = webapp.WSGIApplication([
						('/', OverviewHandler),
						('/about', AboutHandler),
						('/saveq', SaveQueryHandler),
						('/updateq', UpdateQueryHandler),
						('/execq', ExecQueryHandler),
						('/deleteq', DeleteQueryHandler),
						('/datasets', ImportHandler),
						('/upload', UploadHandler),
						('/gcuploads', GarbageCollectUploadsHandler),
						('/api', APIHandler),
						('/admin', AdminBQSEndpointHandler)
					],
					debug=False)

def main():
    run_wsgi_app(application)

if __name__ == '__main__':
	main()
