import io
import pandas as pd
import requests
from lxml import etree
from dateutil import parser as dtparser

def fetch_dwml(url: str, user_agent: str, timeout: int = 60) -> requests.Response:
    headers = {"User-Agent": user_agent}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r

def _time_map(root) -> dict:
    m = {}
    for tl in root.xpath(".//time-layout"):
        key = tl.xpath("./layout-key/text()")[0]
        times = [pd.Timestamp(dtparser.parse(t)).tz_convert("UTC")
                 for t in tl.xpath("./start-valid-time/text()")]
        m[key] = times
    return m

def flatten_dwml(resp_bytes: bytes, stamp_utc: str, lat: float, lon: float) -> pd.DataFrame:
    """
    Returns a wide DataFrame with:
      scrape_time_utc, location_lat, location_lon, forecast_time_utc, and many feature columns.
    One row per forecast timestamp across ALL series in the XML.
    """
    root = etree.fromstring(resp_bytes)
    tmap = _time_map(root)

    # collect all numeric series from all <parameters>
    series = []
    for params in root.xpath(".//parameters"):
        for node in params:
            if not isinstance(node.tag, str):
                continue
            tag = node.tag
            if tag in {"weather", "conditions-icon"}:
                continue
            if not node.xpath("./value"):
                continue
            tl = node.get("time-layout")
            if not tl or tl not in tmap:
                continue
            vals = [v.text for v in node.xpath("./value")]
            times = tmap[tl]
            n = min(len(vals), len(times))
            if n == 0:
                continue
            name = f"{tag}_{node.get('type')}" if node.get("type") else tag
            s = pd.Series(vals[:n], index=pd.to_datetime(times[:n], utc=True), dtype="object")
            s = s.replace({"": pd.NA, "NA": pd.NA, None: pd.NA})
            s = pd.to_numeric(s, errors="coerce")
            series.append((name, s))

    # weather flags (rain/thunder/fog)
    rain = pd.Series(dtype="boolean"); thunder = pd.Series(dtype="boolean"); fog = pd.Series(dtype="boolean")
    wnode = root.xpath(".//parameters/weather")
    if wnode:
        node = wnode[0]
        tl = node.get("time-layout")
        times = tmap.get(tl, [])
        conds = node.xpath("./weather-conditions")
        m = min(len(times), len(conds))
        if m:
            idx = pd.to_datetime(times[:m], utc=True)
            def flags(wc):
                txt = " ".join([
                    " ".join(wc.xpath(".//@weather-type")),
                    " ".join(wc.xpath(".//@intensity")),
                    " ".join(wc.xpath(".//@coverage")),
                    " ".join(wc.xpath(".//@additive")),
                ]).lower()
                return (
                    any(k in txt for k in ["rain","shower","drizzle"]),
                    any(k in txt for k in ["thunder","tstm","tstorm"]),
                    any(k in txt for k in ["fog","mist"])
                )
            fl = [flags(conds[i]) for i in range(m)]
            rain = pd.Series([x[0] for x in fl], index=idx, dtype="boolean")
            thunder = pd.Series([x[1] for x in fl], index=idx, dtype="boolean")
            fog = pd.Series([x[2] for x in fl], index=idx, dtype="boolean")

    # master index (union of all timestamps)
    all_idx = [s.index for _, s in series]
    for extra in (rain.index, thunder.index, fog.index):
        if len(extra): all_idx.append(extra)
    if not all_idx:
        raise RuntimeError("No timestamps found in DWML")
    master = pd.Index([]);  [master := master.union(ix) for ix in all_idx]
    master = master.sort_values()

    # wide frame
    df = pd.DataFrame(index=master)
    for name, s in series:
        df[name] = s.reindex(df.index)
    if len(rain):    df["weather_rain"] = rain.reindex(df.index)
    if len(thunder): df["weather_thunder"] = thunder.reindex(df.index)
    if len(fog):     df["weather_fog"] = fog.reindex(df.index)

    df.reset_index(inplace=True)
    df.rename(columns={"index":"forecast_time_utc"}, inplace=True)
    df.insert(0, "scrape_time_utc", pd.Timestamp(stamp_utc))
    df.insert(1, "location_lat", lat)
    df.insert(2, "location_lon", lon)

    # friendly names
    rename = {
        "temperature_hourly": "temp_F",
        "temperature_apparent": "heat_index_F",
        "dewpoint_hourly": "dewpoint_F",
        "wind-speed_sustained": "wind_speed_mph",
        "wind-speed_gust": "wind_gust_mph",
        "direction": "wind_dir_deg",
        "probability-of-precipitation": "pop_pct",
        "cloud-amount": "sky_cover_pct",
        "humidity_relative": "rh_pct",
        "pressure_sea-level": "pressure_hPa",
        "visibility": "visibility_mi",
        "cig": "ceiling_ft",
    }
    df.rename(columns={k:v for k,v in rename.items() if k in df.columns}, inplace=True)
    df.sort_values("forecast_time_utc", inplace=True)
    return df