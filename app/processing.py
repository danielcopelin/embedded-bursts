import os
from lxml import html
import requests
from io import BytesIO, TextIOWrapper, StringIO
import pandas as pd

os.environ["HTTP_PROXY"] = "165.225.98.34:80"
os.environ["HTTPS_PROXY"] = "165.225.98.34:80"
lat, lon = (-33.869929, 151.205608)

def get_ifd(lat, lon):
    url = "http://www.bom.gov.au/water/designRainfalls/revised-ifd"
    params = {
        "year": "2016",
        "coordinate_type": "dd",
        "latitude": lat,
        "longitude": lon,
        "sdmin": "true",
        "sdhr": "true",
        "sdday": "true",
        "user_label": "",
    }

    ifd_page = requests.get(url=url, params=params)
    tree = html.fromstring(ifd_page.content)
    csv_url = url + tree.xpath('//a[@class="ifdDownloadCsv csvDownloadIcon"]')[0].get("href")

    csv_file = StringIO(requests.get(csv_url).content.decode("utf-8"))
    ifd_df = pd.read_csv(csv_file, skiprows=range(9))
    ifd_df.set_index(pd.to_timedelta(ifd_df["Duration in min"], unit="m"), inplace=True)
    ifd_df = ifd_df.drop(["Duration", "Duration in min"], axis=1)

    return ifd_df

def get_tp(lat, lon):
    url = f"http://embedded-bursts.herokuapp.com/temporal/{lat}/{lon}"
    tp_df = pd.read_html(url)[0]
    tp_df = tp_df.drop([c for c in tp_df.columns if "Unnamed" in c], axis=1)
    tp_df.Duration = pd.to_timedelta(tp_df.Duration, unit="m")
    tp_df.TimeStep = pd.to_timedelta(tp_df.TimeStep, unit="m")
    tp_df.Increments = tp_df.Increments.apply(eval)

    return tp_df

def find_embedded_bursts(tp_df, ifd_df):
    embedded_bursts_data = []

    for row in tp_df.iterrows():
        pattern = row[1]
        event_duration, freq = (pattern.Duration, pattern.TimeStep)
        periods = event_duration / freq
        pattern_index = pd.timedelta_range(end=event_duration, periods=periods, freq=freq)
        pattern_series = pd.Series(data=pattern.Increments, index=pattern_index) / 100.0
        
        matching_ifd_durations = ifd_df.index[(ifd_df.index % freq == pd.Timedelta(0)) & (ifd_df.index < event_duration)]
        for aep in ifd_df.columns:
            event_depth = ifd_df.loc[event_duration, aep]
            event_series = pattern_series * event_depth
            for burst_duration in matching_ifd_durations:
                burst_depth = ifd_df.loc[burst_duration, aep]
                burst_rainfall_sum = event_series.rolling(window=burst_duration).sum()
                embedded_bursts = (burst_rainfall_sum > burst_depth)
                if embedded_bursts.any():
                    burst_idxs = embedded_bursts.reset_index(drop=True)[embedded_bursts.reset_index(drop=True)].index.tolist()
                    embedded_bursts_data.append({
                        "EventID": pattern.EventID,
                        "aep": aep,
                        "event_duration": event_duration, 
                        "event_depth": event_depth, 
                        "burst_duration": burst_duration, 
                        "burst_depth": burst_depth, 
                        "embedded_burst_positions": burst_idxs,

                    })

    embedded_bursts_df = pd.DataFrame(embedded_bursts_data)
    embedded_bursts_df = embedded_bursts_df[["EventID", "aep", "event_duration", "event_depth", "burst_duration", "burst_depth", "embedded_burst_positions"]]
    return embedded_bursts_df


ifd_df = get_ifd(lat, lon)
tp_df = get_tp(lat, lon)
embedded_bursts_df = find_embedded_bursts(tp_df, ifd_df)
