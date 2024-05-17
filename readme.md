python dif_demo.py --runner DirectRunner --temp_location gs://us-bkt-data/df_temp --input gs://us-bkt-data/absence_demo.csv --output dataflow-d:dif_sink.absence --congif gs://us-bkt-data/config.yaml


------------------------------------
gcloud config get-value project


gcloud builds submit --tag gcr.io/dataflow-d/dataflow/dif_csv_to_bq:latest

gcloud beta dataflow flex-template build gs://us-bkt-data/templates/dif_csvtobq_template.json --image gcr.io/dataflow-d/dataflow/dif_csv_to_bq:latest --sdk-language "PYTHON" --metadata-file "metadata.json"

gcloud dataflow flex-template run csvtobq-with-yaml --region us-central1 --template-file-gcs-location gs://us-bkt-data/templates/dif_csvtobq_template.json --parameters input=gs://us-bkt-data/absence_demo.csv --parameters output=dataflow-d:dif_sink.absence1 --parameters config=gs://us-bkt-data/config.yaml