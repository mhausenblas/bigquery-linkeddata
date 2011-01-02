import logging, cgi, os, platform, sys, urllib, boto, time

from django.utils import simplejson as json
from google.appengine.ext import webapp
from google.appengine.api import memcache
from google.appengine.api import users

from util.bqs_access import *
from util.bqs_global import *
from util.bqs_bqwrapper import *
from util.bqs_queryutil import *
from util.bqs_elements import *
from util.nt2csv import *

from bqs_models import *

class APIHandler(webapp.RequestHandler):
	
	RESULT_FORMAT_SIMPLE = "simple"
	RESULT_FORMAT_SPARQL = "SPARQL"
	
	
	def get(self):
		# execute the query if a query string is present:
		qstr = self.request.get('query')
		logging.info('[BQE API] call with params:"%s" from IP:%s' %(self.request.query_string, os.environ['REMOTE_ADDR']))
		if qstr:
			self.handle_query(qstr)
		elif 'metadata' in self.request.query_string:
			self.handle_metadata()
		else: # no operation selected
			self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
			self.response.headers.add_header("Access-Control-Allow-Origin", "*")
			self.response.out.write(self.error_simple_results_JSON("no op"))

	def handle_query(self, qstr):
		resultformat = self.request.get('format')
		(is_valid, vmsg) = self.validate_query(qstr) # check if we have a valid query
		if is_valid:
			# ... hence we gonna execute the query:
			bqw = BigQueryWrapper()
			start = time.time()
			(schema, results) = bqw.execquery(qstr)
			elapsed = (time.time() - start)
			# ... and check which result format has been requested ...
			if 'SPARQL' in resultformat.upper(): # format contains SPARQL in any form ...
				queryresults = self.as_SPARQL_results_JSON(qstr, schema, results, elapsed)
				self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
				#self.response.headers['Content-Type'] = 'application/sparql-results+json; charset=utf-8'
			else: # no format param or any other vaule for format param means: simple result
				queryresults = self.as_simple_results_JSON(qstr, schema, results, elapsed)
				self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
			# provide CORS-enabled results, see also http://enable-cors.org/
			self.response.headers.add_header("Access-Control-Allow-Origin", "*")
			self.response.out.write(queryresults)
		else: # someone is trying to query some other table
			self.response.headers['Content-Type'] = 'application/json; charset=utf-8'
			self.response.headers.add_header("Access-Control-Allow-Origin", "*")
			self.response.out.write(self.error_simple_results_JSON(vmsg))

	def validate_query(self, qstr):
		if not 'SELECT' in qstr.upper():
			return (False, "Only SELECT queries are supported in the BQE API")
			
		allowed_table = "/".join([GlobalUtility.IMPORT_BUCKET, GlobalUtility.RDFTABLE_OBJECT])
		if not allowed_table in qstr:
			return (False, "BQE API calls are restricted to the default table [%s]" %allowed_table)
			
		return (True, "valid") # query looks valid
		
	def as_simple_results_JSON(self, qstr, schema, results, elapsed):
		if type(schema).__name__=='DatabaseError':
		     return self.error_simple_results_JSON("%s" %schema)
		elif type(schema).__name__=='HTTPError':
		     return self.error_simple_results_JSON("%s" %schema)
		
		rmetacontent = dict([('result_format', APIHandler.RESULT_FORMAT_SIMPLE), ('original_query', qstr), ('execution_time_in_s', str(elapsed))])
		variables = []
		for field in schema:
			variables.append(field[0])	
		rcontent = dict([('fields', variables), ('rows', results)])
		retval = dict([('metadata', rmetacontent), ('data', rcontent)])
		return json.JSONEncoder().encode(retval)
		
	def as_SPARQL_results_JSON(self, qstr, schema, results, elapsed):
		# see http://www.w3.org/TR/rdf-sparql-json-res/
		if type(schema).__name__=='DatabaseError':
		     return self.error_simple_results_JSON("%s" %schema)
		elif type(schema).__name__=='HTTPError':
		     return self.error_simple_results_JSON("%s" %schema)
		
		# pre the vars for header
		variables = []
		for field in schema:
			variables.append(field[0])
		head = dict([('vars', variables)])
		
		logging.info("RESULTS: %s" %results)
		# prep the bindings for results
		bindingres = []
		for row in results:
			for var in variables:
				cval = row[variables.index(var)]
				ctype = ''
				# determine type (the following is a hack based on sniffing and not even complete)
				if '://' in cval:
					ctype = 'uri'
				elif '_:' in cval:
					ctype = 'bnode'
					cval = cval.split('_:')[1]
				else:
					ctype = 'literal'
				cell = dict([('value', cval), ('type', ctype)])
				res = dict([(var, cell)])
				bindingres.append(res)

		bindings = dict([('bindings', bindingres)])
		
		retval = dict([('head', head), ('results', bindings)])
		return json.JSONEncoder().encode(retval)

	def error_simple_results_JSON(self, msg):
		retval = dict([('error', str(msg))])
		return json.JSONEncoder().encode(retval)
		
	def handle_metadata(self):
		bqw = BigQueryWrapper()
		exactnumtriples = bqw.numtriples()
		
		void_ttl = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix void: <http://rdfs.org/ns/void#> .
@prefix : <#> .

 :bqe-all rdf:type void:Dataset ;
          foaf:homepage <http://bqs-endpoint.appspot.com/> ;
          dcterms:title "BigQuery Endpoint" ;
          dcterms:description "A wrapper and public access point for BigQuery tables and queries with LOD support." ;
          void:triples %s .
""" %exactnumtriples
		
		self.response.headers['Content-Type'] = 'text/turtle; charset=utf-8'
		self.response.headers.add_header("Access-Control-Allow-Origin", "*")
		self.response.out.write(void_ttl)
