import json
from bokeh.driving import count
from bokeh.layouts import column
from bokeh.models import (
  GMapPlot, GMapOptions, ColumnDataSource, Circle,
  DataRange1d, PanTool, WheelZoomTool, BoxSelectTool,
  HoverTool
)
from bokeh.plotting import curdoc

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.errors import OffsetOutOfRangeError

# This is a nasty, ugly workaround to load the config from a single location:
# the parent directory.
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

def create_map():
    # Programmatically create a tooltip that pops up when hovering
    # a Tweet on the map.
    hover = HoverTool(tooltips=[
        ("Latitude", "@lat"),
        ("Longitude", "@lon"),
        ("Sentiment", "@sentiment"),
        ("Accuracy", "@sentiment_accuracy"),
        ("User", "@name"),
        ("Text", "@text"),
    ])
    # TODO Embed tweet preview?

    google_map_style = [
        {
            "featureType": "all",
            "stylers": [
                {
                    "saturation": 0
                },
                {
                    "hue": "#e7ecf0"
                }
            ]
        },
        {
            "featureType": "road",
            "stylers": [
                {
                    "saturation": -70
                }
            ]
        },
        {
            "featureType": "transit",
            "stylers": [
                {
                    "visibility": "off"
                }
            ]
        },
        {
            "featureType": "poi",
            "stylers": [
                {
                    "visibility": "off"
                }
            ]
        },
        {
            "featureType": "water",
            "stylers": [
                {
                    "visibility": "simplified"
                },
                {
                    "saturation": -60
                }
            ]
        }
    ]

    map_options = GMapOptions(lat=33.0, lng=-83.0, map_type="roadmap",
                              styles=json.dumps(google_map_style), zoom=4)
    plot = GMapPlot(
        x_range=DataRange1d(), y_range=DataRange1d(), map_options=map_options,
        api_key = config.GOOGLE_MAPS_API_KEY
    )
    plot.title.text = "Live Twitter Sentimap"
    plot.add_tools(PanTool(), WheelZoomTool(), BoxSelectTool(), hover)
    return plot

def create_kafka_input(server, topic):
    consumer = None
    try:
        consumer = KafkaConsumer(group_id="sentimap_group", bootstrap_servers=[server],
                                 auto_offset_reset="latest",
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe([topic])
    except Exception as e:
        print("Got exception %s for Kafka: %s".format(str(e), type(e)))
        consumer = None
    finally:
        return consumer

map_plot = create_map()
kafka_consumer = create_kafka_input("localhost:9092", "twitter_sentimap")

def shutdown_hook():
    if kafka_consumer:
        kafka_consumer.close()

source = ColumnDataSource(
    data=dict(
        lat=[],
        lon=[],
        color=[],
        sentiment_accuracy=[],
        sentiment=[],
        text=[],
        name=[],
    )
)

circle = Circle(x="lon", y="lat", size=15, fill_color="color",
                fill_alpha="sentiment_accuracy", line_color="black", line_alpha=0.8,
                line_dash="solid")
map_plot.add_glyph(source, circle)

@count()
def update(t):
    if not kafka_consumer:
        return

    kafka_msgs = None
    try:
        kafka_msgs = kafka_consumer.poll(timeout_ms=200)
    except OffsetOutOfRangeError:
        print("Offset out of range. Seeking to begining")
        # consumer.seek_to_beginning(tp)
        # You can save `consumer.position(tp)` to redis after this,
        # but it will be saved after next message anyway
        return

    if not kafka_msgs:
        return

    # Create an empty dictionary to hold our new data.
    new_chunk = {
        "lat": [],
        "lon": [],
        "color": [],
        "sentiment_accuracy": [],
        "sentiment": [],
        "text": [],
        "name": []
    }

    # Fill the |new_chunK| with oru data.
    for msgs in list(kafka_msgs.values()):
        for msg in msgs:
            print('got msg: {}'.format(json.dumps(msg.value)))
            tweet_data = msg.value

            # Only plot on the map if "location" is available.
            location = tweet_data.get("location")
            if not location:
                continue

            new_chunk["lat"].append(location[0])
            new_chunk["lon"].append(location[1])
            sentiment = tweet_data.get("sentiment")
            new_chunk["sentiment"].append(sentiment[0])
            new_chunk["sentiment_accuracy"].append(sentiment[1])
            new_chunk["color"].append("red" if sentiment[0] == "negative" else "green")
            new_chunk["text"].append(tweet_data.get("text", ""))
            new_chunk["name"].append(tweet_data.get("screen_name", ""))

    source.stream(new_chunk, 1000)

curdoc().add_root(column(map_plot, sizing_mode="scale_width"))
curdoc().add_periodic_callback(update, 1000)
curdoc().title = "Twitter sentimap"
