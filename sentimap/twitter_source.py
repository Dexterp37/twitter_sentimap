import config
import json
import logging
import tweepy


logger = logging.getLogger(__name__)


class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self, data_sink):
        # Store the data sink that will receive the data events.
        # We want to encapsulate the Twitter logic in here and let
        # the sink deal with the rest.
        self._data_sink = data_sink

    def on_data(self, data):
        if not self._data_sink:
            # If the sink doesn't exit, bail out.
            return False

        # Decode the JSON data and close the stream if we can't.
        tweet = None
        try:
            tweet = json.loads(data)
        except ValueError:
            logger.exception("Malformed tweet data received.")
            return False

        self._data_sink.on_data_available(tweet)

    def on_error(self, status):
        pass


class TwitterSource:
    def __init__(self):
        self._auth = None
        self._stream = None

    def start(self, languages, keywords):
        if self._stream:
            logger.error("The stream is already started.")
            return

        self._stream = tweepy.Stream(self._auth, TwitterStreamListener(self))
        self._stream.filter(track=keywords, languages=languages, async=True)

    def stop(self):
        if not self._stream:
            logger.error("The stream was not started.")
            return

        self._stream.disconnect()

    def on_data_available(self, json_data):
        print("Received data: {}\n".format(json.dumps(json_data)))
