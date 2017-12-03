# Twitter Sentimap

This project implements a social media monitoring prototype platform that performs real-time sentiment analysis of live data feeds. It provides a map of the sentiment and social engagement toward particular keywords and tags.

## Technological stack
In order to implement the real-time sentiment dashboard, the following technological stack was used:

- Twitter REST API through tweepy, a Python library for fetching live tweet feeds;
- NLTK for the natural language processing capabilities (stemming and tokenization);
- Keras (an higher level API that uses Tensorflow) and genism to create a Word2Vec CNN model for sentiment analysis;
- Kafka for message passing between the analysis module and the visualization module
- Google Maps cartography data;
- Bokeh for real-time streaming visualization of geotagged data.

