#################################################
# Dependencies
#################################################
import pandas as pd
import datetime as dt
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify


#################################################
# Database Setup
################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create session
session = Session(engine)

#################################################
# Queries
#################################################
# Find last date
most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
most_recent_date = dt.date.fromisoformat(most_recent_date_str)

# Find one year ago
one_year_ago = most_recent_date - dt.timedelta(days=365)

# Find the precipitation amounts for the last year
last_12_precip = session.query(Measurement.date, func.max(Measurement.prcp)). \
    filter(Measurement.date >= func.strftime("%Y-%m-%d", one_year_ago)). \
    group_by(Measurement.date). \
    order_by(Measurement.date).all()

# Save results as a DF and set the date as the index
df = pd.DataFrame(last_12_precip, columns=['date', 'prcp'])
df.set_index('date', inplace=True)

# Use Pandas to calculate the summary statistics for the precipitation data
last_12_precip_qry = session.query(Measurement.date, Measurement.prcp). \
    filter(Measurement.date >= func.strftime("%Y-%m-%d", one_year_ago)). \
    order_by(Measurement.date).all()

last_12_precip_df = pd.DataFrame(last_12_precip_qry, columns=['date', 'prcp'])
last_12_precip_df.set_index('date', inplace=True)

last_12_precip_max = last_12_precip_df.groupby(["date"]).max()["prcp"]
last_12_precip_min = last_12_precip_df.groupby(["date"]).min()["prcp"]
last_12_precip_count = last_12_precip_df.groupby(["date"]).count()["prcp"]

last_12_precip_dict = {"Max": last_12_precip_max, "Min": last_12_precip_min, "Count": last_12_precip_count}

last_12_precip_summary_df = pd.DataFrame(last_12_precip_dict)

# Design a query to calculate the total number stations in the dataset
number_of_stations = session.query(Station.station).count()

# Find the most active station and list the station and its count
most_active_stations_qry = session.query(Measurement.station, func.count(Measurement.station)). \
    group_by(Measurement.station). \
    order_by(func.count(Measurement.station).desc())

all_active_stations = most_active_stations_qry.all()

# Using the most active station id, find it's min, max, and avg temperature.
most_active_station_id = most_active_stations_qry.first()[0]

temp_summary = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)). \
    filter(Measurement.station == most_active_station_id).all()

# Using the most active station id find its temperature data from the last year
tobs_last_12_qry = session.query(Measurement.date, Measurement.tobs). \
    filter(Measurement.date >= func.strftime("%Y-%m-%d", one_year_ago), Measurement.station == most_active_station_id). \
    order_by(Measurement.date).all()

tobs_last_12_df = pd.DataFrame(tobs_last_12_qry, columns=['date', 'tobs'])
tobs_last_12_df.set_index('date', inplace=True)

stations_qry = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
stations_df = pd.DataFrame(stations_qry, columns=['station', 'name', 'latitude', 'longitude', 'elevation'])
stations_df.set_index('station', inplace=True)

# Close Session
session.close()

app = Flask(__name__)


#################################################
# Routes
#################################################
@app.route("/")
def index():
    return (
        f"All available routes:<br/><br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    result = {}
    for index, row in last_12_precip_summary_df.iterrows():
        result[index] = dict(row)
    return jsonify(result)


@app.route("/api/v1.0/stations")
def stations():
    result = {}
    for index, row in stations_df.iterrows():
        result[index] = dict(row)
    return jsonify(result)


@app.route("/api/v1.0/tobs")
def tobs():
    result = {}
    for index, row in tobs_last_12_df.iterrows():
        result[index] = dict(row)
    return jsonify(result)


@app.route("/api/v1.0/<start>")
def start_date(start):
    try:
        # Attempt to convert the string to a datetime object
        start = dt.date.fromisoformat(start)
    except ValueError:
        # Handle incorrect date format
        return jsonify({"error": "Invalid date format. Please use ISO format (YYYY-MM-DD)."}), 400

    session = Session(engine)
    start_date_query = session.query(
        func.max(Measurement.tobs).label("TMAX"),
        func.avg(Measurement.tobs).label("TAVG"),
        func.min(Measurement.tobs).label("TMIN")
    ).filter(Measurement.date >= start).all()

    start_date_df = pd.DataFrame(start_date_query, columns=['TMAX', 'TAVG', 'TMIN'])
    result = start_date_df.iloc[0].to_dict()

    session.close()
    return jsonify(result)


@app.route("/api/v1.0/<start>/<end>")
def between_range(start, end):
    try:
        # Attempt to convert strings to datetime objects
        start = dt.date.fromisoformat(start)
        end = dt.date.fromisoformat(end)
    except ValueError:
        # Handle incorrect date format
        return jsonify({"error": "Invalid date format. Please use ISO format (YYYY-MM-DD)."}), 400

    session = Session(engine)
    qry_between_range = session.query(
        func.max(Measurement.tobs).label("TMAX"),
        func.avg(Measurement.tobs).label("TAVG"),
        func.min(Measurement.tobs).label("TMIN")
    ). \
        filter(Measurement.date >= start, Measurement.date <= end).all()

    between_range_df = pd.DataFrame(qry_between_range, columns=['TMAX', 'TAVG', 'TMIN'])
    result = between_range_df.iloc[0].to_dict()

    session.close()
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5500)
