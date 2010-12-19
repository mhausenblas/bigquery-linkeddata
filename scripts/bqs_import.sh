python tools/nt2csv.py tmp/$1
gsutil cp -a public-read tmp/$2 gs://lod-cloud/in/$2
gsutil getacl gs://lod-cloud/in/$2