import geocoder
import logging

logger = logging.getLogger(__name__)


class TweetToPacket:
    """ Transform a raw Tweet to a more useful data packet

    This class cleans raw Twitter data and removes undesired fields.
    """
    def __init__(self, data_callback=None):
        self._callback = data_callback

    def process_input(self, data):
        if not data or len(data) != 1:
            logger.error("Empty or invalid data")
            return

        # Clean and extract the data.
        raw_tweet = data[0]
        cleaned = {
            "location": self._get_geo_coords(raw_tweet),
            "text": raw_tweet.get("text"),
            "tweet_id": raw_tweet.get("id"),
            "user_id": raw_tweet.get("user", {}).get("id"),
            "screen_name": raw_tweet.get("user", {}).get("screen_name"),
        }

        # Pass it on.
        self.on_data_available(cleaned)

    def set_data_available_callback(self, data_callback):
        self._callback = data_callback

    def on_data_available(self, data):
        if self._callback:
            self._callback(data)

    def _get_geo_coords(self, data):
        """ Extract a latitude and a longitude from a tweet.

        A tweet may contain different location cues:
          - "geo": an object with latitude and longitude;
          - "coordinates": geoJSON object with longitude and latitude; it represents
            the location of the specific tweet;
          - "place": a location the tweet is associated with (not necessarily
            where it was originated);
          - "user.location": an user defined location for the account;
          - "user.time_zone": the timezone string can contains cues for the rough
            location of the user;
          - "user.utc_offset": the utc offset can contain cues too
        """

        # Check and pass geo.
        geo = data.get("geo")
        if geo and geo.get("coordinates"):
            coords = geo.get("coordinates")
            return (coords[0], coords[1])

        coords = data.get("coordinates")
        if coords:
            inner_coords = coords.get("coordinates")
            # This has inverted lat and long.
            return (inner_coords[1], inner_coords[0])

        place_data = data.get("place")
        if place_data and place_data.get("bounding_box"):
            bbox_coords = place_data.get("bounding_box", {}).get("coordinates")
            if len(bbox_coords) >= 1:
                one_point = bbox_coords[0][0]
                return (one_point[1], one_point[0])

            raise ValueError("Implement place")

        user_data = data.get("user")
        if not user_data:
            logger.debug("No user data in this tweet")
            return None

        if user_data.get("location"):
            loc = geocoder.google(user_data.get("location"))
            if loc.latlng:
                return loc.latlng

        utc_offset = user_data.get("utc_offset")
        if utc_offset:
            # This field contains the offset from GMT/UTC
            # in seconds. Convert it to hours. The sign
            # tells us the direction from Europe: negative
            # means toward the US, positive toward Asia.
            utc_hours = utc_offset / 60.0 / 60.0

            # Each UTC hour is a 15° slice of the world.
            longitude = utc_hours * 15.0
            # We can't deduce the latitude, so just assign one
            latitude =  36.778259
            return (latitude, longitude)

        if user_data.get("time_zone"):
            logger.debug("Implement time_zone")

        # This tweet had nothing we could use.
        return None
