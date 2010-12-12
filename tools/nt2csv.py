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
		"""
		Initialises the converter. If a .nt file is given, the converter uses this as an input to create a .csv file. 
		However, if a directory is specified, then all .nt files in this directory are converted to .csv files.
		"""
		self.ifname = fname
		self.dodir = os.path.isdir(fname) # flag to remember if we're working on a directory or single file

	def create_outfilename(self, infilename):
		(workpath, outfilename) = os.path.split(infilename)
		(filename, ext) = os.path.splitext(outfilename) 
		return os.path.join(workpath, filename + '.csv') 
		
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

	def convertfile(self, ifile):
		"""
		Takes an RDF file in U{NTriples format <http://www.w3.org/TR/rdf-testcases/#ntriples>} and turns it into a U{BigQuery-compliant <http://code.google.com/apis/bigquery/docs/uploading.html#createtabledata>}  CSV file.
		"""
		ofile = self.create_outfilename(ifile)
		
		#: prep output so that the CSV is compliant with 
		bqcsv = csv.writer(open(ofile, 'wb'), delimiter=',', quoting=csv.QUOTE_ALL)

		#: read RDF NTriples content line by line ...
		try:
			rdftriples = csv.reader(open(ifile, 'rb'), delimiter=' ')
			for triple in rdftriples:
				#: ... and create CSV in BigQuery CSV format
				bqcsv.writerow([self.strip_anglebr(triple[0]), self.strip_anglebr(triple[1]), self.strip_anglebr(self.strip_datatype(triple[2]))])
		except csv.Error, e:
		    sys.exit('file %s, line %d: %s' % (ofile, rdftriples.line_num, e))

		print 'Done converting ' + ifile + ' to ' + ofile

	def convert(self):
		"""
		Depending on the initialisation, converts a single NTriple file or an entire directory with NTriple files to CSV.
		"""
		if self.dodir == True: # process *.nt in directory
			input_files = [f for f in os.listdir(self.ifname) if os.path.isfile(os.path.join(self.ifname, f)) and f.endswith('.nt')]
		 	for f in input_files:
				self.convertfile(os.path.join(self.ifname, f))
		else: # process a single .nt file
			self.convertfile(self.ifname)

def main():
	if sys.argv[1] == '-h':
		print ' INPUT: specify the RDF/NTriple file to use as an input'
		print ' OUTPUT: a BigQuery-compliant CSV file with the same name as the input file'
		print ' --------------------------------------------------------------------------'
		print ' EXAMPLE: python tools/nt2csv.py test/mhausenblas-foaf.nt'
	else:
		nt2csv = NTriple2CSV(sys.argv[1])
		nt2csv.convert()
		
if __name__ == '__main__':
	main()