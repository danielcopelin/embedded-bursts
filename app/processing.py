import os
import zipfile
from io import BytesIO, StringIO, TextIOWrapper

import pandas as pd
import requests
from lxml import html


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
    csv_url = url + tree.xpath('//a[@class="ifdDownloadCsv csvDownloadIcon"]')[0].get(
        "href"
    )

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


def get_tp(lat, lon):
    base_url = "https://data.arr-software.org/"
    query_url = base_url + "?lon_coord={0}&lat_coord={1}&type=json&All=1"

    # lon, lat = (152.648, -29.573)

    r = requests.get(query_url.format(lon, lat))
    response_json = r.json()

    point_tp_url = base_url + response_json["layers"]["PointTP"]["url"]
    d = requests.get(point_tp_url, stream=True)
    zipdata = BytesIO()
    zipdata.write(d.content)
    myzipfile = zipfile.ZipFile(zipdata)
    increments_file = [
        f for f in myzipfile.namelist() if f.endswith("_Increments.csv")
    ][0]

    df = pd.read_csv(TextIOWrapper(myzipfile.open(increments_file)))
    df.columns = [c.strip() for c in df.columns.values]
    df["Increments"] = pd.Series(df.iloc[:, 5:].values.tolist())
    df.Increments = df.Increments.apply(lambda row: [r for r in row if str(r) != "nan"])
    df.Duration = pd.to_timedelta(df.Duration, unit="m")
    df.TimeStep = pd.to_timedelta(df.TimeStep, unit="m")
    df = df.drop([c for c in df.columns.values if "Unnamed" in c], axis=1)

    return df


def find_embedded_bursts(tp_df, ifd_df):
    aep_ranges = {
        "rare": ["1%", "2%"],	    #Rare   Rarer than 3.2% AEP
        "intermediate": ["5%", "10%"], #Intermediate	Between 3.2% and 14.4% AEP
        "frequent": ["20%", "50%", "63.2%"],     #Frequent	More frequent than 14.4% AEP
    }

    embedded_bursts_data = []

    for row in tp_df.iterrows():
        pattern = row[1]
        pattern_aep = pattern.AEP
        event_duration, freq = (pattern.Duration, pattern.TimeStep)
        periods = event_duration / freq
        pattern_index = pd.timedelta_range(
            end=event_duration, periods=periods, freq=freq
        )
        pattern_series = pd.Series(data=pattern.Increments, index=pattern_index) / 100.0

        matching_ifd_durations = ifd_df.index[
            (ifd_df.index % freq == pd.Timedelta(0)) & (ifd_df.index < event_duration)
        ]
        for aep in [c for c in ifd_df.columns if c in aep_ranges[pattern_aep]]:
            event_depth = ifd_df.loc[event_duration, aep]
            event_series = pattern_series * event_depth
            for burst_duration in matching_ifd_durations:
                burst_ifd_depth = ifd_df.loc[burst_duration, aep]
                burst_rainfall_sum = event_series.rolling(window=burst_duration).sum()
                embedded_bursts = burst_rainfall_sum > burst_ifd_depth
                if embedded_bursts.any():
                    burst_idxs = burst_rainfall_sum.reset_index(drop=True)[
                        embedded_bursts.reset_index(drop=True)
                    ].to_dict()
                    embedded_bursts_data.append(
                        {
                            "EventID": pattern.EventID,
                            "aep": aep,
                            "event_duration": event_duration,
                            "event_depth": event_depth,
                            "burst_duration": burst_duration,
                            "burst_ifd_depth": burst_ifd_depth,
                            "embedded_burst_positions_and_depths": burst_idxs,
                        }
                    )

    embedded_bursts_df = pd.DataFrame(embedded_bursts_data)
    embedded_bursts_df = embedded_bursts_df[
        [
            "EventID",
            "aep",
            "event_duration",
            "event_depth",
            "burst_duration",
            "burst_ifd_depth",
            "embedded_burst_positions_and_depths",
        ]
    ]
    return embedded_bursts_df

if __name__ == "__main__":
    lat, lon = (-27, 153)
    tp_df = get_tp(lat, lon)
    ifd_df = get_ifd(lat, lon)
    embedded_bursts_df = find_embedded_bursts(tp_df, ifd_df)