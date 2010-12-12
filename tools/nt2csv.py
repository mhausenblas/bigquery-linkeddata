"""
A converter from RDF/NTriples format into BigQuery-compliant CSV format. 

This tool is part of the U{BigQuery for Linked Data <http://code.google.com/p/bigquery-linkeddata/>} effort.

@author: Michael Hausenblas, http://sw-app.org/mic.xhtml#i
@since: 2010-12-12
@status: inital version
"""

import sys, csv, os

"""
The NTriple/CSV converter class.
"""
class NTriple2CSV:
	def __init__(self,fname):
		(self.workpath, self.ofname) = os.path.split(fname)
		self.ifname = fname
		(filename, ext) = os.path.splitext(self.ofname) 
		self.ofname = filename + '.csv'
	def strip_anglebr(self,text):
		"""
		Strips angle barckets from string, that is <...> into ...
		@return: the string with '<'...'>' removed
		"""
		if text.startswith('<'):
			text = text.strip('<')
		if text.endswith('>'):
			text = text.strip('>') 
		return text
	def strip_datatype(self,text):
		"""
		Removes NTriple datatype, that is, it turns '...'^^<http://www.w3.org/2001/XMLSchema#string> into '...'
		@return: the string with '^^<...>' removed
		"""
		if text.find('^^') >= 0:
			text = text.split('^^')
			return text[0]
		else:
			return text
	def convert(self):
		"""
		Takes an RDF file in U{NTriples format <http://www.w3.org/TR/rdf-testcases/#ntriples>} and turns it into a U{BigQuery-compliant <http://code.google.com/apis/bigquery/docs/uploading.html#createtabledata>}  CSV file.
		@return: none
		"""
		#: prep output so that the CSV is compliant with 
		bqcsv = csv.writer(open(os.path.join(self.workpath, self.ofname), 'wb'), delimiter=',', quoting=csv.QUOTE_ALL)

		#: read RDF NTriples content line by line ...
		try:
			rdftriples = csv.reader(open(self.ifname, 'rb'), delimiter=' ')
			for triple in rdftriples:
				#: ... and create CSV in BigQuery CSV format
				bqcsv.writerow([self.strip_anglebr(triple[0]), self.strip_anglebr(triple[1]), self.strip_anglebr(self.strip_datatype(triple[2]))])
		except csv.Error, e:
		    sys.exit('file %s, line %d: %s' % (self.ofname, rdftriples.line_num, e))

		print 'Done! Converted ' + self.ifname + ' to ' + self.ofname

def main():
	if sys.argv[1] == '-h':
		print " INPUT: specify the RDF/NTriple file to use as an input"
		print " OUTPUT: a BigQuery-compliant CSV file with the same name as the input file"
		print " --------------------------------------------------------------------------"
		print " EXAMPLE: python nt2csv.py ../test/mhausenblas-foaf.nt"
	else:
		nt2csv = NTriple2CSV(sys.argv[1])
		nt2csv.convert()
		
if __name__ == '__main__':
	main()