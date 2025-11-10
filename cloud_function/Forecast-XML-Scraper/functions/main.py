import os
import functions_framework
from scraper.pipeline import Pipeline

# ---- Env config (override in deploy flags) ----
PROJECT_ID = os.getenv("PROJECT_ID", "indigo-cider-469519-j2")
GCS_BUCKET = os.getenv("GCS_BUCKET", "nws-poc-indigo-cider-469519-j2")
RAW_PREFIX = os.getenv("RAW_PREFIX", "nws_raw/")
CSV_PREFIX = os.getenv("CSV_PREFIX", "nws_flat/")

LAT = float(os.getenv("LAT", "41.94"))
LON = float(os.getenv("LON", "-72.685"))
FCST_TYPE = os.getenv("FCST_TYPE", "digitalDWML")
USER_AGENT = os.getenv("USER_AGENT", "nws-forecast-poc/1.0 (akole.rohit07@gmail.com)")

DWML_URL = (
    f"https://forecast.weather.gov/MapClick.php?"
    f"lat={LAT}&lon={LON}&unit=0&lg=english&FcstType={FCST_TYPE}"
)

pipe = Pipeline(
    project_id=PROJECT_ID,
    bucket_name=GCS_BUCKET,
    raw_prefix=RAW_PREFIX,
    csv_prefix=CSV_PREFIX,
    lat=LAT, lon=LON,
    fcst_url=DWML_URL,
    user_agent=USER_AGENT
)

@functions_framework.http
def scrape_dwml(request):
    result = pipe.run_once()
    return result