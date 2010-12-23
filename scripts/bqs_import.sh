python tools/nt2csv.py tmp/$1
gsutil cp -a public-read tmp/$2 gs://lodcloud/in/$2
bq import lodcloud/rdftable lodcloud/in/$2
