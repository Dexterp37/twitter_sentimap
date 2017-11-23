import json
import logging
import os


logger = logging.getLogger(__name__)


# How many tweets to put in a single JSON file? This
# directly influences.
ENTRIES_PER_FILE = 500


class SourceRecorder:
    """ Write the incoming data to a set of valid JSON files.

    The data is written in chunks, with each file containing
    ENTRIES_PER_FILE number of tweet.
    """
    def __init__(self, data_dir):
        self._data_dir = data_dir
        self._buffer = []
        self._file_counter = 0

        # Make sure the data_dir exists.
        if not os.path.exists(self._data_dir):
            os.makedirs(self._data_dir)

    def _flush_buffer(self):
        # Generate a filename for this chunk.
        file_name = "chunk-{}.json".format(self._file_counter)
        file_path = os.path.join(self._data_dir, file_name)

        # Save the data from the buffer to the file.
        with open(file_path, 'wt') as outfile:
            json.dump(self._buffer, outfile)

        # Empty the buffer and increase the file counter.
        self._buffer = []
        self._file_counter = self._file_counter + 1

    def on_data_available(self, json_data):
        logger.debug("Received data: {}\n".format(json.dumps(json_data)))

        self._buffer.append(json_data)

        if len(self._buffer) > ENTRIES_PER_FILE:
            self._flush_buffer()
