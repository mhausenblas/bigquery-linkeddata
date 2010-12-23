In order to perform the following steps, you need a Google Storage as well as a BigQuery account.
Let us further assume that you've checked out the source from https://bigquery-linkeddata.googlecode.com/hg/ 
into the $bgl directory, which we will use as the base directory in the following. 

Step 1: Prepare the data file

You'll need an RDF file in NTriples format, either from the test/ directory
or grab one from the Web (for example enter 'rdf filetype:nt' into Google). 
Once you have the RDF file in a local directory (we assume it's in test/),
run the following command (in $bgl):

 python tools/nt2csv.py test/mhausenblas-foaf.nt

which creates the file test/mhausenblas-foaf.csv we will use in the next step.

Step 2: Upload the data file

You upload the data file to Google Storage into one of your buckets, shown as 'mybucket' in the following;
replace it with the actual name of your bucket.

 gsutil cp test/mhausenblas-foaf.csv gs://mybucket/in/mhausenblas-foaf.csv

Step 3: Create the RDF table

To create the table that holds the RDF data, you need to specify a table schema: 

 bq create mybucket/rdftable schema/quintuple.scheme 

The layout of the RDF table looks like the following:

 +--------------------------------------------------------+
 | graph_uri | subject | predicate | object | object_type |
 +--------------------------------------------------------+
 | ...       | ...     | ...       | ...    | ...         |

Step 4: Import the RDF data into the table

Next you have to import the earlier created CSV file into the table:

 bq import mybucket/rdftable mybucket/in/mhausenblas-foaf.csv

Importing can take up to 10 minutes if you have a lot of data.

Step 5: Run a query

Once the CSV file is imported into the table, we can run a query:

$ bq query "SELECT object FROM [mybucket/rdftable] WHERE predicate = 'http://xmlns.com/foaf/0.1/knows' LIMIT 10"

The query above lists ten people that I know, which corresponds to the following SPARQL query:

 PREFIX foaf: <http://xmlns.com/foaf/0.1/>

 SELECT ?o
 FROM <http://any23.org/rdfxml/http://sw-app.org/mic.xhtml>
 WHERE {
  ?s foaf:knows ?o .
 }
 LIMIT 10

