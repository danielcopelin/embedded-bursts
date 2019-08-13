from app import app, processing
from app.forms import LatLonForm
from flask import render_template, flash, redirect


@app.route("/", methods=["GET", "POST"])
def index():
    form = LatLonForm()
    if form.validate_on_submit():
        # flash("Latitude: {}, Longitude={}".format(form.lat.data, form.lon.data))
        return redirect(f"/temporal/{form.lat.data}/{form.lon.data}")
    return render_template("index.html", title="ARR Temporal Pattners", form=form)


@app.route("/temporal/<lat>/<lon>")
def show_tp(lat, lon):
    tp_df = processing.get_tp(lat, lon)
    ifd_df = processing.get_ifd(lat, lon)
    embedded_bursts_df = processing.find_embedded_bursts(tp_df, ifd_df)
    return render_template(
        "temporal.html",
        title="ARR Temporal Pattners",
        tables=[embedded_bursts_df.to_html(classes="data", header="true")],
    )
