import csv
import logging
from contextlib import closing
from io import TextIOWrapper

import boto3
import pyarrow.parquet as pq
import s3fs

from direct_fulfillment_speed.entities.nodes import (
    Carrier,
    Destination,
    Node,
    Vendor,
    VendorManager,
    Warehouse,
)
from direct_fulfillment_speed.entities.shipment import ShipmentClass, ShipmentInstance
from direct_fulfillment_speed.utils import util
from direct_fulfillment_speed.utils.config import ConfigManager

logger = logging.getLogger()


class InputStream:
    """Class for data stream from a file."""

    def __init__(
        self, bucket_name, object_key, delimiter=",", header=None, input_file_format: str = "csv"
    ):
        """
        Initializes the stream from an S3 bucket.

        :param bucket_name: The name of the S3 bucket.
        :param object_key: The key of the object in the S3 bucket.
        :param delimiter: The delimiter used in the CSV file.
        :param header: Optional list of header fieldnames.
        """
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.delimiter = delimiter
        self.s3fs = s3fs.S3FileSystem()
        self.header = header  # Predefined header or None
        self.input_file_format = input_file_format

    def header_exists(self, line):
        return False if self.header else True

    def process_records(self):
        if self.input_file_format.lower() == "csv":
            return self.process_records_from_csv()
        elif self.input_file_format.lower() == "parquet":
            return self.process_records_from_parquet()
        else:
            raise ValueError(f"Unsupported file format: {self.input_file_format}")

    def process_records_from_csv(self):
        with closing(
            self.s3.get_object(Bucket=self.bucket_name, Key=self.object_key)["Body"]
        ) as s3_file:
            # Wrap the streaming body in TextIOWrapper for text mode reading and specify the correct encoding
            with TextIOWrapper(s3_file, encoding="utf-8") as text_wrapper:
                # Use csv.DictReader if headers are provided or expected in the first row
                if self.header:
                    reader = csv.DictReader(
                        text_wrapper, delimiter=self.delimiter, fieldnames=self.header
                    )
                else:
                    reader = csv.DictReader(text_wrapper, delimiter=self.delimiter)

                for record in reader:
                    yield record

    def process_records_from_parquet(self, batch_size=500000):
        file_path = f"{self.bucket_name}/{self.object_key}"
        with self.s3fs.open(file_path, "rb") as file_obj:
            parquet_file = pq.ParquetFile(file_obj)
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                df_chunk = batch.to_pandas()
                records = df_chunk.to_dict(orient="records")
                for record in records:
                    yield record


class ReadInputs:
    """Read and process input data from an S3 bucket."""

    def __init__(self, config: ConfigManager):
        self.config = config
        self.vendor_manager = VendorManager()

    def read_shipments(
        self,
        delimiter=",",
    ) -> ShipmentClass:
        """
        Reads shipment data from an S3 bucket and creates Shipment instances.
        """

        path = self.config.input_path
        bucket_name, object_key = util.parse_s3_path(path)
        logger.info(f"Reading shipment data from {path}")

        input_stream = InputStream(
            bucket_name, object_key, delimiter, input_file_format=self.config.input_format
        )

        # Initialize the shipment class
        ship_obj = ShipmentClass(self.config)

        # Initialize the Node class
        nodes_obj = Node()

        for record in input_stream.process_records():
            if (
                not record.get("destination_zip5")
                or record.get("destination_zip5").isspace()
                or "*" in record.get("destination_zip5")
            ):
                logger.info(f"Skipping record due to invalid destination_zip5: {record}")
                continue  # Skip the records with empty dest zip5

            # Skip records missing order datetime fields
            if not record.get("order_datetime") or not record.get("of_datetime"):
                logger.info(f"Skipping record due to missing datetime fields: {record}")
                continue

            # Create the vendor using the vendor manager
            vendor = self.vendor_manager.get_or_create_vendor(
                record["vendor_id"], record.get("vendor_primary_gl_description", "Unknown")
            )

            warehouse = Warehouse(
                vendor,
                record["warehouse_id"],
                record["origin_zip5"],
            )

            carrier = Carrier(
                record.get("carrier", ""),
                record.get("ship_method_orig", ""),
                record.get("ship_method_1", ""),
            )

            destination = Destination(zip5=record.get("destination_zip5"))

            # Create a Shipment instance
            shipment_instance = ShipmentInstance(
                record["region_id"],
                record["marketplace_id"],
                record["shipment_id"],
                record["order_id"],
                record["tracking_id"],
                record["package_id"],
                vendor,
                warehouse,
                carrier,
                destination,
                record.get("ship_method_1"),
                record.get("order_datetime"),
                record.get("of_datetime"),
                record.get("is_fasttrack", False),
                record.get("c2exsd", None),
                record.get("c2p_days", None),
                record.get("c2p_days_unpadded", None),
                record.get("c2d_days", None),
                record.get("att_c2d_days", None),
                record.get("att_failed_pdd", False),
                record.get("no_att_scan", False),
                record.get("unpadded_pdd_date", None),
                record.get("tt_pad", None),
                record.get("gl_group", "Unknown"),
                record.get("vendor_primary_gl_description", "Unknown"),
                record.get("distance_mi", 0),
            )

            ship_obj.add_shipment(shipment_instance)
            nodes_obj.add_warehouse(vendor, warehouse)

        return ship_obj
