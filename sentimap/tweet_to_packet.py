import config
import geocoder
import keras.backend as K
import logging
import numpy as np
import tensorflow as tf
from gensim.models.word2vec import Word2Vec
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Flatten
from keras.layers.convolutional import Conv1D
from nltk.stem.lancaster import LancasterStemmer
from nltk.tokenize import RegexpTokenizer

logger = logging.getLogger(__name__)


class TweetToPacket:
    """ Transform a raw Tweet to a more useful data packet

    This class cleans raw Twitter data and removes undesired fields.
    """
    def __init__(self, data_callback=None):
        self._callback = data_callback
        # The Word2Vec sentiment analysis of Twitter data was
        # taken from:
        # https://github.com/giuseppebonaccorso/twitter_sentiment_analysis_word2vec_convnet
        self._tokenizer = RegexpTokenizer('[a-zA-Z0-9@]+')
        self._stemmer = LancasterStemmer()
        # Load our pre-trained models.
        self._w2v = self._load_word2vec_keyed_vectors()
        self._model = self._load_keras_model()
        # The below is needed due to how Keras handle async stuff. See
        # https://github.com/fchollet/keras/issues/2397#issuecomment-254919212
        self._model_graph = tf.get_default_graph()

    def process_input(self, data):
        if not data or len(data) != 1:
            logger.error("Empty or invalid data")
            return

        raw_tweet = data[0]

        # Clean and extract the data.
        tokens, features = self._process_text(raw_tweet)
        cleaned = {
            "location": self._get_geo_coords(raw_tweet),
            "text": raw_tweet.get("text"),
            "text_tokens": tokens,
            "sentiment": self._get_sentiment(features),
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

    def _load_word2vec_keyed_vectors(self):
        word2vec = Word2Vec.load(config.WORD2VEC_MODEL_PATH)
        # We only want to keep the KeyedVectors instance.
        w2v_keyed_vectors = word2vec.wv
        # Save some memory by dumping the rest of the model.
        del word2vec
        return w2v_keyed_vectors

    def _load_keras_model(self, max_tweet_tokens=15, word2vec_vector_size=512):
        # We didn't store the model structure in the file when saving our model.
        # Let's redefine it here.
        model = Sequential()

        model.add(Conv1D(32, kernel_size=3, activation='elu', padding='same',
                         input_shape=(max_tweet_tokens, word2vec_vector_size)))
        model.add(Conv1D(32, kernel_size=3, activation='elu', padding='same'))
        model.add(Conv1D(32, kernel_size=3, activation='elu', padding='same'))
        model.add(Conv1D(32, kernel_size=3, activation='elu', padding='same'))
        model.add(Dropout(0.25))

        model.add(Conv1D(32, kernel_size=2, activation='elu', padding='same'))
        model.add(Conv1D(32, kernel_size=2, activation='elu', padding='same'))
        model.add(Conv1D(32, kernel_size=2, activation='elu', padding='same'))
        model.add(Conv1D(32, kernel_size=2, activation='elu', padding='same'))
        model.add(Dropout(0.25))

        model.add(Flatten())

        model.add(Dense(256, activation='tanh'))
        model.add(Dense(256, activation='tanh'))
        model.add(Dropout(0.5))

        model.add(Dense(2, activation='softmax'))

        # Load the weights.
        model.load_weights(config.KERAS_MODEL_PATH)

        return model

    def _process_text(self, data, max_tweet_tokens=15, word2vec_vector_size=512):
        text = data.get("text")
        if not text:
            logger.error("This Tweet contains no text.")
            return None

        # Strip whitespaces and make the tweet lowercase.
        tokens = [self._stemmer.stem(t)
                  for t in self._tokenizer.tokenize(text.strip().lower())
                  if not t.startswith('@')]

        # TODO: Remove stop words from here and from the model!

        # Compute the feature vector for this Tweet. For each token and up to the
        # maximum number of tokens allowed (must match the number used when training
        # the model!) get the word embedding from our Word2Vec model.
        features = np.zeros((1, max_tweet_tokens, word2vec_vector_size), dtype=K.floatx())

        for i, token in enumerate(tokens):
            # Stop if we got enough features.
            if i >= max_tweet_tokens:
                break

            # This feature is not in our w2v model. Try the next one.
            if token not in self._w2v:
                continue

            features[0, i, :] = self._w2v[token]

        return tokens, features

    def _get_sentiment(self, features):
        # Perform the prediction. We need to make sure the default graph
        # is the one we loaded, because we might be in another thread.
        pred = None
        with self._model_graph.as_default():
            # We trained the model in batches, but we want the prediciton for
            # a single sample. Make sure to set the batch_size so Keras doesn't
            # complain.
            pred = self._model.predict(features, batch_size=1)

        # Interpret the predicted result: if the index of the maximum value
        # is 0, then this Tweet has a negative sentiment. If it's 1, then it's
        # mostly positive.
        sentiment = np.argmax(pred[0])
        # Return the sentiment value and its "likelihood".
        return (
            "negative" if sentiment == 0 else "positive",
            np.asscalar(pred[0][sentiment]),
        )

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
            latitude = 36.778259
            return (latitude, longitude)

        if user_data.get("time_zone"):
            logger.debug("Implement time_zone")

        # This tweet had nothing we could use.
        return None
