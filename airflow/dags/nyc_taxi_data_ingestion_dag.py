from airflow.sdk import dag, task
from pendulum import datetime, instance
from airflow.timetables.interval import CronDataIntervalTimetable
import urllib.request
import boto3
from datetime import timedelta

S3_BUCKET_NAME = "airflow-demo-bucket-june8"
TAXI_TYPES = ["yellow", "green"]

@dag(
    dag_id="nyc_taxi_data_ingestion_dag",
    schedule=CronDataIntervalTimetable("0 6 1 * *", timezone="Asia/Kolkata"),
    start_date=datetime(year=2014, month=4, day=1, tz="Asia/Kolkata"),
    catchup=True,
    max_active_runs=2,
    max_active_tasks=4,
)
def nyc_taxi_data_ingestion_dag():

    @task.python
    def get_date_parts(data_interval_start=None):
        delay_date = instance(data_interval_start).subtract(months=3)
        print(f"Interval start: {data_interval_start} → Fetching: {delay_date}")
        return {
            "year": delay_date.strftime("%Y"),
            "month": delay_date.strftime("%m")
        }

    @task.python(
            retries=3,
            retry_delay=timedelta(minutes=5) # wait 5 min before retrying
    )
    def download_and_upload(parts: dict, taxi_type: str):
        year, month = parts["year"], parts["month"]
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
        s3_key = f"final/{taxi_type}/year={year}/month={month}/{taxi_type}_tripdata_{year}-{month}.parquet"

        print(f"Streaming {taxi_type} {year}-{month} → s3://{S3_BUCKET_NAME}/{s3_key}")

        req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
    )

        s3 = boto3.client("s3")

        with urllib.request.urlopen(req) as response:
            # Multipart upload — streams in chunks, never loads full file into RAM
            mpu = s3.create_multipart_upload(Bucket=S3_BUCKET_NAME, Key=s3_key)
            upload_id = mpu["UploadId"]
            parts_list = []
            part_number = 1

            try:
                while True:
                    chunk = response.read(8 * 1024 * 1024)  # 8MB chunks
                    if not chunk:
                        break
                    part = s3.upload_part(
                        Bucket=S3_BUCKET_NAME,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts_list.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    part_number += 1

                s3.complete_multipart_upload(
                    Bucket=S3_BUCKET_NAME,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts_list},
                )
                print(f"Done: s3://{S3_BUCKET_NAME}/{s3_key}")

            except Exception as e:
                # Always abort incomplete multipart uploads to avoid S3 storage charges
                s3.abort_multipart_upload(
                    Bucket=S3_BUCKET_NAME, Key=s3_key, UploadId=upload_id
                )
                raise e

    parts = get_date_parts()
    for taxi_type in TAXI_TYPES:
        download_and_upload.override(
            task_id=f"upload_{taxi_type}_taxi_data"
        )(parts=parts, taxi_type=taxi_type)


nyc_taxi_data_ingestion_dag()