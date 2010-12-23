class GlobalUtility:
	# the link for the help menu item:
	HELP_LINK = 'http://webofdata.wordpress.com/2010/12/13/processing-linked-open-data-with-bigquery/'
	
	# access credentials for Google Storage (you need to put your own here, see https://sandbox.google.com/storage/m/manage)
	GS_ACCESS_KEY_ID = 'XXX'
	GS_SECRECT_ACCESS_KEY = 'XXX'
	# you need to put your Google Storage/BigQuery account credentials here:
	GS_ENABLED_GMAIL_ADDRESS = 'XXX@gmail.com'
	GS_ENABLED_GMAIL_PWD = 'XXX'
	
	# the base Google Storage URI
	GOOGLE_STORAGE_BASE_URI = 'http://commondatastorage.googleapis.com'
	# the bucket for converted CSV files
	IMPORT_BUCKET = 'lodcloud'
	# the object to hold converted CSV files
	IMPORT_OBJECT = 'in'
	# the object to hold the RDF table
	RDFTABLE_OBJECT = 'rdftable'
	# data postfix
	DATA_POSTFIX = 'csv'
	# metadata postfix
	METADATA_POSTFIX = 'meta'
	
	# the target graph if nothing is specified
	DEFAULT_GRAPH_URI = 'http://example.org/default'
	# a default query for the input
	DEFAULT_QUERY_STRING = "SELECT subject, predicate, object FROM [lodcloud/rdftable] LIMIT 3"
	# a default query to count the number of triples
	DEFAULT_QUERY_COUNT = "SELECT COUNT(*) FROM [lodcloud/rdftable]"