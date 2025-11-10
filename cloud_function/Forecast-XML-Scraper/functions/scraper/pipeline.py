import io, datetime as dt
import pandas as pd
from google.cloud import storage
from .dwml_parse import fetch_dwml, flatten_dwml

class Pipeline:
    def __init__(self, *, project_id: str, bucket_name: str,
                 raw_prefix: str, csv_prefix: str,
                 lat: float, lon: float, fcst_url: str, user_agent: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.raw_prefix = raw_prefix
        self.csv_prefix = csv_prefix
        self.lat = lat
        self.lon = lon
        self.fcst_url = fcst_url
        self.user_agent = user_agent

        self.storage = storage.Client(project=project_id)
        self.bucket = self.storage.bucket(bucket_name)

    def run_once(self) -> dict:
        stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        # 1) fetch and save raw
        resp = fetch_dwml(self.fcst_url, self.user_agent)
        raw_name = f"{self.raw_prefix}dwml_{stamp}.xml"
        self.bucket.blob(raw_name).upload_from_string(resp.text, content_type="application/xml")

        # 2) flatten
        df = flatten_dwml(resp.content, stamp, self.lat, self.lon)

        # 3) write per-run CSV
        per_run = f"{self.csv_prefix}flat_{stamp}.csv"
        buf = io.StringIO(); df.to_csv(buf, index=False)
        self.bucket.blob(per_run).upload_from_string(buf.getvalue(), content_type="text/csv")

         # We no longer maintain a rolling master.csv or push to BigQuery.
        return {
            "raw_xml": f"gs://{self.bucket_name}/{raw_name}",
            "per_run_csv": f"gs://{self.bucket_name}/{per_run}",
            "rows_this_run": len(df),
        }