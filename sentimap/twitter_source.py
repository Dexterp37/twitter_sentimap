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

    def on_error(self, status_code):
        # Returning False in on_data disconnects the stream. This
        # is needed when the Twitter API returns 420: it means that
        # we're exceeding the number of attempts.
        logger.error("An error was received from the Twitter API.", extra_data={status_code})
        if status_code == 420:
            return False


class TwitterSource:
    def __init__(self, data_callback=None):
        self._auth = tweepy.OAuthHandler(config.TWITTER_CONSUMER_KEY,
                                         config.TWITTER_CONSUMER_SECRET)
        self._auth.set_access_token(config.TWITTER_ACCESS_TOKEN,
                                    config.TWITTER_ACCESS_SECRET)
        self._stream = None
        self._callback = data_callback

    def start(self, languages, keywords):
        if self._stream:
            logger.error("The stream is already started.")
            return

        self._stream = tweepy.Stream(self._auth, TwitterStreamListener(self), stall_warnings=True)
        self._stream.filter(track=keywords, languages=languages, async=True)

    def stop(self):
        if not self._stream:
            logger.error("The stream was not started.")
            return

        self._stream.disconnect()

    def set_data_available_callback(self, data_callback):
        self._callback = data_callback

    def on_data_available(self, json_data):
        if self._callback:
            # Send a vector of 1 for compatibility with the replay source.
            # We can configure it to send more than one tweet!
            self._callback([json_data])
