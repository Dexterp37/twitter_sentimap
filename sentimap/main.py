import argparse
import logging
import sys
from twitter_source import TwitterSource
from replay_source import ReplaySource
from source_recorder import SourceRecorder


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyse and display the sentiment of live Twitter streams.")

    parser.add_argument("--record", dest="record", action="store_true",
                        required=False, default=False,
                        help="Enable recording the live data to the 'datadir' directory.")
    parser.add_argument("--replay", dest="replay", action="store_true",
                        required=False, default=False,
                        help="Enable replaying the live data from the 'datadir' directory.")
    # If we're recording or replaying, make sure to require the datadir.
    parser.add_argument("--datadir", action="store",
                        required=any(x in sys.argv for x in ["--record", "--replay"]),
                        help="The directory for the recorded/replayed data")

    parser.add_argument("--log-level", action="store", required=False,
                        default="info",
                        help="Set the default log level (e.g. INFO, WARNNG, ...)")

    return parser.parse_args()


def execute_pipeline(args):
    # Set the desired log level.
    logging.basicConfig(level=args.log_level.upper())
    logger.info("Starting")

    source = None
    # Pick the right data source depending on the provided arguments.
    if args.replay:
        source = ReplaySource(args.datadir,
                              chunk_size=1,
                              chunk_delay=2)
        source.set_data_available_callback(lambda d: logger.info("Replayed record"))
    else:
        source = TwitterSource()

        # Are we plannng to record the Twitter stream?
        if args.record:
            recorder = SourceRecorder(args.datadir)
            source.set_data_available_callback(recorder.on_data_available)

    # Start fetching tweets.
    source.start(['en'], ['trump'])

    # Wait for enter to quit.
    input("--> Press enter to quit.")

    source.stop()


if __name__ == "__main__":
    # Parse the command line arguments and build a pipeline.
    args = parse_args()
    execute_pipeline(args)
