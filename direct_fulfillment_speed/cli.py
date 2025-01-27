import argparse
import datetime as dt
import logging
import sys

from direct_fulfillment_speed.inputs.read_inputs import ReadInputs
from direct_fulfillment_speed.optimization.predict import Predict
from direct_fulfillment_speed.optimization.speed_optimizer import Optimize
from direct_fulfillment_speed.outputs.print_outputs import ProcessOutputs
from direct_fulfillment_speed.utils import util
from direct_fulfillment_speed.utils.config import ConfigManager

# Set logger
logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s,%(msecs)d %(levelname)-8s \
            [%(filename)s:%(lineno)d] %(message)s",
    level=logging.INFO,
)


def set_log_level(log_mode="INFO"):
    logger.setLevel(log_mode)


logger = logging.getLogger()


def parse_cmd_line():
    parser = argparse.ArgumentParser(description="Your Project Command Line Interface.")
    parser.add_argument(
        "--config_file",
        "-c",
        dest="config_file",
        help="Path to config file.",
        default="../resources/config.ini",
    )

    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        default="../output",
        help="Choose the output local destination directory.",
    )

    parser.add_argument(
        "-l",
        "--location_choice",
        choices=["local", "s3", "both"],
        default="s3",
        help="Choose the output location: 'local' for local storage, 's3' for S3 bucket, or 'both' for both.",
    )

    args = parser.parse_args()

    return args


def main():
    # Track starting time
    start_time = dt.datetime.now()

    args = parse_cmd_line()

    # Load the configuration using AppConfig
    config = ConfigManager(args.config_file, args.location_choice, args.output)
    logger.info(f"Loaded config file from {args.config_file}")

    # Set log level using the new config class
    set_log_level(config.log_mode)

    # Read inputs shipments
    logger.info("Start reading input data...")
    inputs = ReadInputs(config)
    shipments_object = inputs.read_shipments()
    logger.info(f"Reading input data is done.")

    logger.info(f"Start processing input data...")
    shipments_object.update_shipment_counts()
    shipments_object.extract_ods_warehouse_metrics()  # Get ODS/SWA metrics like UNPADDED_DEA, C2D, c2p_unpadded
    logger.info(f"Processing input data is done.")

    # Perform prediction
    logger.info(f"Start performing the prediction...")
    predict_obj = Predict(config, shipments_object)
    predict_obj.perform_forecasts()
    logger.info(f"Finished prediction.")

    # Optimize
    logger.info(f"Start performing the optimization...")
    optimization_obj = Optimize(shipments_object, predict_obj, config)
    optimization_obj.solve()
    logger.info(f"Optimization done.")

    # Print output
    ProcessOutputs(config, predict_obj, optimization_obj)

    end_time = dt.datetime.now()
    logger.info(f"Total run time {util.get_clockwise_time_diff(start_time, end_time)} seconds")


if __name__ == "__main__":
    main()
