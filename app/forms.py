from flask_wtf import FlaskForm
from wtforms import FloatField, SubmitField
from wtforms.validators import DataRequired

class LatLonForm(FlaskForm):
    lat = FloatField('Latitude', validators=[DataRequired()])
    lon = FloatField('Longitude', validators=[DataRequired()])
    submit = SubmitField('Submit')
