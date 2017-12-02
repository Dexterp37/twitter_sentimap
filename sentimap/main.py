import argparse
import json
import logging
import sys
from kafka_output import KafkaOutput
from twitter_source import TwitterSource
from tweet_to_packet import TweetToPacket
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
    parser.add_argument("--fakegeo", action="store_true", default=False,
                        help="Enable faking latitude and longitude")

    parser.add_argument("--loglevel", action="store", required=False,
                        default="info",
                        help="Set the default log level (e.g. INFO, WARNNG, ...)")

    parser.add_argument("--kafkaserver", action="store", required=False, default=None,
                        help="The address of the kafka server to send the data to")

    parser.add_argument("--keyword", action="store", required="--replay" not in sys.argv,
                        default=None,
                        help="The keyword to listen to on social media")

    return parser.parse_args()


def execute_pipeline(args):
    # Set the desired log level.
    logging.basicConfig(level=args.loglevel.upper())
    logger.info("Starting")

    # Pick the right data source depending on the provided arguments.
    source = None
    if args.replay:
        source = ReplaySource(args.datadir,
                              chunk_size=1,
                              chunk_delay=2,
                              add_fake_geo=args.fakegeo)
    else:
        source = TwitterSource()

    # Set-up a sink that will deal with our Twitter data.
    if not args.replay and args.record:
        # Are we plannng to record the Twitter stream?
        recorder = SourceRecorder(args.datadir)
        source.set_data_available_callback(recorder.on_data_available)

    # Define the function to send the output to Kafka.
    kafka_writer = KafkaOutput("twitter_sentimap", args.kafkaserver) if args.kafkaserver else None

    # Clean the raw tweets.
    cleaner = TweetToPacket(kafka_writer.process_input
                            if kafka_writer
                            else lambda t: logger.debug("Clean record {}".format(json.dumps(t))))
    source.set_data_available_callback(cleaner.process_input)

    # Run the pipeline and bail out when the user presses ENTER.
    source.start(['en'], [args.keyword])
    input("--> Press enter to quit.\n")
    source.stop()


if __name__ == "__main__":
    # Parse the command line arguments and build a pipeline.
    args = parse_args()
    execute_pipeline(args)
