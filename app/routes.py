from app import app
from flask import render_template

import zipfile
from io import BytesIO, TextIOWrapper

import pandas as pd

import requests

def get_tp():
    base_url = "https://data.arr-software.org/"
    query_url = base_url+"?lon_coord={0}&lat_coord={1}&type=json&All=1"

    lon, lat = (152.648, -29.573)

    r = requests.get(query_url.format(lon, lat))
    response_json = r.json()

    point_tp_url = base_url+response_json['layers']['PointTP']['url']
    d = requests.get(point_tp_url, stream=True)
    zipdata = BytesIO()
    zipdata.write(d.content)
    myzipfile = zipfile.ZipFile(zipdata)
    increments_file = [f for f in myzipfile.namelist() if f.endswith("_Increments.csv")][0]

    df = pd.read_csv(TextIOWrapper(myzipfile.open(increments_file)))
    df.columns = [c.strip() for c in df.columns.values]
    df["Increments"] = pd.Series(df.iloc[:,5:].values.tolist())
    df.Increments = df.Increments.apply(lambda row: [r for r in row if str(r) != 'nan'])
    df = df.drop([c for c in df.columns.values if "Unnamed" in c], axis=1)

    return df

@app.route('/')
def show_tp():
    return render_template("index.html", title="ARR Temporal Pattners", tables=[get_tp().to_html(classes='data', header="true")])
