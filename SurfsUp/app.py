# ToDo: rearrange queries and routes to stop repeated lines of code...
#################################################
# Dependencies
#################################################
import datetime as dt
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
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

# Find last date
most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
most_recent_date = dt.date.fromisoformat(most_recent_date_str)

# Find one year ago
one_year_ago = most_recent_date - dt.timedelta(days=365)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################
@app.route("/")
def index():
    """All available api routes"""
    return ("Welcome to the Climate API! <br><br>"
            "Available Routes:<br>"
            "/api/v1.0/precipitation<br/>"
            "/api/v1.0/stations<br/>"
            "/api/v1.0/tobs<br/>"
            "/api/v1.0/&lt;start&gt;<br/>"
            "/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
            )

# precipitation- only returns data for the last year
@app.route("/api/v1.0/precipitation")
def precipitation():
    # session = Session(engine)
    #
    # # Find last date
    # most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
    # most_recent_date = dt.date.fromisoformat(most_recent_date_str)
    #
    # one_year_ago = most_recent_date - dt.timedelta(days=365)

    # query to retrieve the data and precipitation scores
    precipitation_results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.prcp.isnot(None)).filter(Measurement.date >= one_year_ago).order_by(Measurement.date).all()

    # save query in dictionary
    precipitation_dictionary = {str(result[0]): float(result[1]) for result in precipitation_results}

    # close session
    session.close()

    # return JSON
    return jsonify(precipitation_dictionary)


@app.route("/api/v1.0/stations")
def stations():
    #"""Return the stations in the database"""
    # create a session
    session = Session(engine)

    # query stations
    results = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()

    # close session
    session.close()

    # create list
    stations_list = []

    for station, name, latitude, longitude, elevation in results:
        stations_dict = {}
        stations_dict['station'] = station
        stations_dict['name'] = name
        stations_dict['latitude'] = latitude
        stations_dict['longitude'] = longitude
        stations_dict['elevation'] = elevation
        stations_list.append(stations_dict)

    return jsonify(stations_list)


@app.route('/api/v1.0/tobs')
def most_active():

    # Create session
    session = Session(engine)

    # Find last date
    most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = dt.date.fromisoformat(most_recent_date_str)

    one_year_ago = most_recent_date - dt.timedelta(days=365)

    # Query the entire tobs for the most active station in the last year
    # ToDo: incorporate query to look up most active station so it's not hardcoded...
    past_year = session.query(Measurement.date, Measurement.tobs).filter_by(station = "USC00519281").filter(Measurement.date >= one_year_ago).all()

    # Close session
    session.close()

    past_year_list = []
    for date, tobs in past_year:
        tobs_dict = {}
        tobs_dict['date'] = date
        tobs_dict['tobs'] = tobs
        past_year_list.append(tobs_dict)
    return jsonify(past_year_list)


@app.route("/api/v1.0/<start>")
# ToDo: Finish this...
def daily_stats(start):
    """Min, max, and avg temperatures from a given date to the end of the data set"""
    # create session
    session = Session(engine)

    # convert iso string to date object
    start_date = dt.date.fromisoformat(start)

    # Find last date
    most_recent_date_str = session.query(func.max(Measurement.date)).scalar()
    most_recent_date = dt.date.fromisoformat(most_recent_date_str)

    # Query the date greater than or equal to start
    calculations = [func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)]

    start_filter = session.query(*calculations).filter(Measurement.date >= start_date).all()
    start_list = [
        {"Min": start_filter[0][0]},
        {"Max": start_filter[0][1]},
        {"Avg": start_filter[0][2]}
    ]
    if start_date <= most_recent_date:
        return jsonify(start_list)
    else:
        return jsonify(f"ERROR: Please enter a date on or before {most_recent_date}")


    # close session
    session.close()

@app.route("/api/v1.0/<start>/<end>")
def temperature_between(start, end):
    """Min, max, and avg temperature between two dates (inclusive)"""

    # convert strings to datetime objects
    start = dt.date.fromisoformat(start)
    end = dt.date.fromisoformat(end)

    # create session
    session = Session(engine)

    # query
    results = session.query(func.min(Measurement.tobs),
                            func.avg(Measurement.tobs),
                            func.max(Measurement.tobs))
    filter(Measurement.date >= start). \
        filter(Measurement.date <= end).all()
    tobs_list_results = [float(results[0][0]), float(results[0][1]), float(results[0][2])]

    # Close the session
    session.close()

    # return query
    return jsonify(tobs_list_results)


if __name__ == '__main__':
    app.run(debug=True, port=5500)
