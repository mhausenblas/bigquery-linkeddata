class GlobalUtility:
	# the link for the help menu item:
	HELP_LINK = 'http://webofdata.wordpress.com/2010/12/13/processing-linked-open-data-with-bigquery/'
	
	# the base Google Storage URI
	GOOGLE_STORAGE_BASE_URI = 'http://commondatastorage.googleapis.com'
	# the bucket for converted CSV files
	IMPORT_BUCKET = 'lod-cloud'
	# the object to hold converted CSV files
	IMPORT_OBJECT = 'in'
	# data postfix
	DATA_POSTFIX = 'csv'
	# metadata postfix
	METADATA_POSTFIX = 'meta'
	
	# the target graph if nothing is specified
	DEFAULT_GRAPH_URI = 'http://example.org/default'