import os
import pathlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Union

import pandas as pd

# Initialize this dictionary to save the dates and keep it teh converted dates
date_cache: Dict[str, Optional[datetime]] = {}


def get_clockwise_time_diff(start_time: datetime, end_time: datetime):
    """Get the time difference between time units without date, by counting clockwise."""
    if start_time > end_time:
        # In the provided fight schedule, there is a chance the end_time is earlier than start_time
        # since dates are not provided.
        end_time += timedelta(days=1)
    return (end_time - start_time).total_seconds()


def create_file_path(
    dir_path: str, folder_name: str, file_name: str, suffix: str
) -> pathlib.PurePath:
    """
    Create a path from dir, folder, file names and suffix type.
    Args:
        dir_path: Path to the directory.
        folder_name: Name of the folder that file will be saved into.
        file_name: File name.
        suffix: Type of file.

    Returns:
        file_address: Path to a file.
    """
    folder_address = pathlib.PurePath(dir_path, folder_name)
    file_address = pathlib.PurePath(folder_address, file_name + "." + suffix)
    return file_address


def create_folder_path(*argv) -> str:
    """
    Create a path address by concatenation of arguments.
    Args:
        *argv: Tuple/List of strings.

    Returns:
        Path to a folder.
    """
    return os.path.join(*argv)


def folder_exists(folder_path: str) -> bool:
    """
        Check if the dir exists or not.
    Args:
        folder_path: Path to the folder.

    Returns:
        Return true if exists.
    """
    return os.path.exists(folder_path)


def parse_s3_path(s3_path):
    """
    Parses the S3 path to extract the bucket name and object key.

    :param s3_path: The full S3 path (e.g., s3://bucket-name/object-key)
    :return: A tuple containing the bucket name and object key
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("Invalid S3 path format. Must start with 's3://'")

    # Remove the 's3://' prefix and split the remaining string by the first '/'
    path_without_scheme = s3_path[5:]
    bucket_name, object_key = path_without_scheme.split("/", 1)

    return bucket_name, object_key


def concat_strings(separator: str, *argv):
    """Create a concatenation of arguments."""
    return str(separator).join(str(arg) for arg in argv)


def convert_time_str_to_dt_object(time_str: str) -> Optional[datetime]:
    """
    Converts a given time string to a datetime object, or returns None if the conversion fails.
    This function uses a cache to avoid redundant conversions.
    """
    # Check if the conversion has already been done
    if time_str in date_cache:
        return date_cache[time_str]

    if not time_str:
        return None

    if isinstance(time_str, pd.Timestamp):
        return time_str

    datetime_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M",
    ]

    for fmt in datetime_formats:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue

    return None


def to_zip5(zip_code) -> str:
    """Convert an integer or string to a string ZIP5."""
    zip_code_str = str(zip_code).strip()

    # Check if the ZIP code is a valid format
    if not zip_code_str.isdigit() or len(zip_code_str) > 5:
        raise ValueError(f"Invalid ZIP code format: {zip_code}")

    return zip_code_str.zfill(5)


def to_zip3(zip5: str) -> str:
    """Extract the ZIP3 from a ZIP5 code."""
    zip5_str = str(zip5).strip()
    if not zip5_str.isdigit() or len(zip5_str) < 3:
        raise ValueError(f"Invalid ZIP format: {zip5}")

    return zip5_str[:3]


def date_now(include_time=False):
    """
    Return date and time now, with optional formatting.
    """
    utc_datetime = datetime.utcnow()

    if include_time:
        return utc_datetime.strftime("%Y-%m-%d %H%M%S")
    else:
        return utc_datetime.strftime("%Y-%m-%d")


def add_days_to_date(date_str: str, days: int, date_format="%Y-%m-%d"):
    """
    Add a specified number of days to a given date string.
    """
    date_obj = datetime.strptime(date_str, date_format)
    new_date_obj = date_obj + timedelta(days=days)
    return new_date_obj.strftime(date_format)


def formatted_days_to_hours(pad):
    """Convert formatted pad days to hours."""
    if isinstance(pad, str):
        pad = float(pad.replace("p", ".").replace("N", "-"))
    return pad * 24


def formatted_days_to_minutes(pad):
    """Convert formatted pad days to minutes."""
    if isinstance(pad, str):
        pad = float(pad.replace("p", ".").replace("N", "-"))
    return pad * 24 * 60
