## Running Spark in the Cloud

### Connecting to Google Cloud Storage 

Uploading data to GCS:

```bash
gsutil -m cp -r pq/ gs://dtc_data_lake_nytaxi/pq
```

Download the jar for connecting to GCS to any location (e.g. the `lib` folder):

```bash
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar
```


### Local Cluster and Spark-Submit

Creating a stand-alone cluster ([docs](https://spark.apache.org/docs/latest/spark-standalone.html)):

```bash
./sbin/start-master.sh
```

Creating a worker:

```bash
URL="insert local url here"
./sbin/start-slave.sh ${URL}

# for newer versions of spark use that:
#./sbin/start-worker.sh ${URL}
```

Edit the script and then run it:

```bash 
python spark_sql.py \
    --output=data/report-2020
```

Use `spark-submit` for running the script on the cluster

```bash
URL="insert local url here"

spark-submit \
    --master="${URL}" \
    spark_sql.py \
        --output=data/report-2021
```

### Data Proc

Upload the script to GCS:

```bash
TODO
```

Params for the job:

* `--input_green=gs://dtc_data_lake-nytaxi/pq/green/2021/*/`
* `--input_yellow=gs://dtc_data_lake-nytaxi/pq/yellow/2021/*/`
* `--output=gs://dtc_data_lake-nytaxi/report-2021`


Using Google Cloud SDK for submitting to dataproc
([link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud))

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    gs://dtc_data_lake-nytaxi/code/06_spark_sql.py \
    -- \
        --input_green=gs://dtc_data_lake-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake-nytaxi/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake-nytaxi/report-2020
```

### Big Query

Upload the script to GCS:

```bash
TODO
```

Write results to big query ([docs](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark)):

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
    gs://dtc_data_lake/code/06_spark_sql_big_query.py \
    -- \
        --input_green=gs://dtc_data_lake/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake-nytaxi/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020
```

