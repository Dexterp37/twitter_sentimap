import json
import logging
import os
import random
from threading import Thread
from time import sleep


logger = logging.getLogger(__name__)


class ReplaySource:
    """ Replay a previously recorded data stream.
    """
    def __init__(self, data_dir, chunk_size, chunk_delay=0.05,
                 data_callback=None, add_fake_geo=False):
        self._data_dir = data_dir
        self._chunk_size = chunk_size
        self._callback = data_callback
        self._running = False
        self._thread = None
        self._chunk_delay = chunk_delay
        self._add_fake_geo = add_fake_geo

    def start(self, languages, keywords):
        if not os.path.exists(self._data_dir):
            logger.error("The data dir doesn't exist.")
            return

        # Spin a thread so that the code is async and consistent
        # with TwitterSource.
        self._running = True
        self._thread = Thread(target=self._run)
        self._thread.start()

    def stop(self):
        if not self._running:
            logger.error("The stream was not started.")
            return

        self._running = False
        # Wait 5 seconds at most for the thread to terminate.
        self._thread.join(5.0)

    def _load_chunk(self, file_path):
        logger.debug("Loading chunk {}".format(file_path))
        if not os.path.exists(file_path):
            logger.error("The file doesn't exist.")
            return []

        # Load the json chunk and return its content.
        data = []
        with open(file_path) as data_file:
            data = json.load(data_file)

        return data

    def _generate_fake_lat_long(self, origin_lat, origin_lon):
        """ Generate a random pair of latitude and longitude.

        This function is useful for debugging purposes. It avoids
        querying the geocoding services that might rate limit us.
        """
        return {
            "coordinates": [
                origin_lat + random.random(),
                origin_lon + random.random()
            ]
        }

    def _run(self):
        """ A loop running in another thread that replays the recorded data.

        This function:
            - reads the data directory;
            - enumerates the contained .json files;
            - reads the file chunk by chunk.
        """
        # Build the list of chunk files.
        file_paths = []
        for file in os.listdir(self._data_dir):
            if file.endswith(".json"):
                file_paths.append(os.path.join(self._data_dir, file))

        if len(file_paths) < 1:
            logger.warning("The datadir is empty. Nothing to replay.")
            self._running = False

        # Replay the content of the chunks.
        file_index = 0
        entry_cursor = 0
        chunk_buffer = self._load_chunk(file_paths[file_index])
        while self._running:
            # Get a chunk of the data and update the cursor position.
            final_cursor_position = entry_cursor + self._chunk_size
            chunk = chunk_buffer[entry_cursor:final_cursor_position]
            entry_cursor = final_cursor_position

            if self._add_fake_geo:
                chunk[0]["geo"] = self._generate_fake_lat_long(42.378036, -71.118340)

            # Run the callback.
            self.on_data_available(chunk)

            # Load the next chunk file if needed.
            if final_cursor_position >= len(chunk_buffer):
                file_index = file_index + 1
                entry_cursor = 0
                chunk_buffer = self._load_chunk(file_paths[file_index])

            # Sleep a bit after each chunk.
            sleep(self._chunk_delay)

    def set_data_available_callback(self, data_callback):
        self._callback = data_callback

    def on_data_available(self, json_data):
        if self._callback:
            self._callback(json_data)
