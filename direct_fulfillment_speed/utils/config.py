import ast
import configparser
import logging
import os
from io import StringIO
from pathlib import Path
from typing import List, Optional

# Set logger
logger = logging.getLogger()


class ConfigManager:
    def __init__(self, config_path: Path, location_choice: str = "s3", outputs_dir: str = ""):
        self.config = self.read_config(config_path)
        self.location_choice = location_choice  # Store the output choice
        self.outputs_dir = outputs_dir

    @staticmethod
    def read_config(file_path: Path):
        _, file_extension = os.path.splitext(str(file_path))
        config = configparser.ConfigParser()

        if file_extension == ".txt":
            with open(str(file_path), "r") as file:
                file_contents = file.read()
            config_file_like = StringIO(file_contents)
            config.read_file(config_file_like)
        elif file_extension == ".ini":
            config.read(str(file_path))  # Convert Path to str for compatibility
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

        return config

    def get(self, section: str, option: str, fallback: Optional[str] = None) -> str:
        """Getter for config values."""
        if fallback is None:
            return self.config.get(section, option)
        else:
            return self.config.get(section, option, fallback=fallback)

    @property
    def output_choice(self) -> str:
        """Retrieve the user's output choice."""
        return self.location_choice

    # INPUTS Section
    @property
    def input_path(self) -> str:
        return self.get("INPUTS", "PATH")

    @property
    def input_format(self) -> str:
        return self.get("INPUTS", "FORMAT")

    @property
    def log_mode(self) -> str:
        return self.get("INPUTS", "LOG_MODE")

    # XPRESS Section
    @property
    def xpress_heursearchrootselect(self) -> int:
        return self.config.getint("XPRESS", "HEURSEARCHROOTSELECT", fallback=-1)

    @property
    def xpress_outputflag(self) -> bool:
        return self.config.getboolean("XPRESS", "OUTPUTFLAG", fallback=True)

    @property
    def xpress_threads(self) -> int:
        return self.config.getint("XPRESS", "THREADS", fallback=4)

    @property
    def epsilon(self) -> float:
        return self.config.getfloat("XPRESS", "EPSILON", fallback=0.01)

    @property
    def xpress_presolve(self) -> int:
        return self.config.getint("XPRESS", "MIPPRESOLVE", fallback=1)

    @property
    def xpress_max_solve(self) -> int:
        return self.config.getint("XPRESS", "MAXTIME", fallback=1000)

    @property
    def integrality_gap_percentage(self) -> float:
        return self.config.getfloat("XPRESS", "IntegralityGapPercentage", fallback=0.00001)

    # MODEL Section
    @property
    def min_unpadded_dea_threshold(self) -> float:
        return self.config.getfloat("MODEL", "UNPADDED_DEA_THRESHOLD", fallback=0.85)

    @property
    def min_ods_count(self) -> int:
        return self.config.getint("MODEL", "MIN_ODS_COUNT")

    @property
    def min_network_dea(self) -> float:
        return self.config.getfloat("MODEL", "MIN_NETWORK_DEA")

    @property
    def min_swa_dea(self) -> float:
        return self.config.getfloat("MODEL", "MIN_SWA_DEA")

    @property
    def min_3p_ground_dea(self) -> float:
        return self.config.getfloat("MODEL", "MIN_3P_GROUND_DEA")

    @property
    def min_3p_air_dea(self) -> float:
        return self.config.getfloat("MODEL", "MIN_3P_AIR_DEA")

    @property
    def min_dea_furniture_swa(self) -> float:
        return self.config.getfloat("MODEL", "MIN_DEA_FURNITURE_SWA")

    @property
    def min_dea_furniture_ups_ground(self) -> float:
        return self.config.getfloat("MODEL", "MIN_DEA_FURNITURE_UPS_GROUND")

    @property
    def min_dea_tires_ups_ground(self) -> float:
        return self.config.getfloat("MODEL", "MIN_DEA_TIRES_UPS_GROUND")

    @property
    def min_pad(self):
        return float(self.config.get("MODEL", "MIN_PAD"))

    @property
    def max_pad(self):
        return float(self.config.get("MODEL", "MAX_PAD"))

    @property
    def max_pad_air(self):
        return float(self.config.get("MODEL", "MAX_PAD_AIR"))

    @property
    def max_pad_swa(self):
        return float(self.config.get("MODEL", "MAX_PAD_SWA"))

    @property
    def lambda_decay(self):
        return float(self.config.get("MODEL", "LAMBDA_DECAY"))

    @property
    def get_model_test_end_date(self):
        return self.config.get("MODEL", "TESTING_END_DATE")

    @property
    def get_gl_list(self):
        return ast.literal_eval(self.config.get("MODEL", "GL_LIST"))

    @property
    def quantile_list(self) -> List[float]:
        try:
            start = int(self.config.get("MODEL", "QUANTILES_RANGE_START"))
            end = int(self.config.get("MODEL", "QUANTILES_RANGE_END"))
            increment = int(self.config.get("MODEL", "QUANTILES_INCREMENT"))

            # Starting the range from the first increment and prepending 1
            quantiles = list(range(start, end + 1, increment))

            # Generate the range and add intermediate values
            current = start
            while current < end:
                current += increment
                if current < end:
                    quantiles.append(current)

            # Add 99 if it's not in the list
            if 99 not in quantiles and 99 < end:
                quantiles.append(99)
            if end not in quantiles:
                quantiles.append(end)

            return [float(q) for q in quantiles]
        except ValueError as e:
            logger.error(f"Error parsing quantiles range from config: {e}")
            return []  # Return an empty list

    # OUTPUTS Section
    @property
    def print_lp_file(self) -> bool:
        return self.config.getboolean("OUTPUTS", "PRINT_LP_FILE")

    @property
    def s3_output_folder(self) -> str:
        return self.get("OUTPUTS", "S3_OUTPUT_PATH")

    @property
    def local_output_folder(self) -> str:
        config_output_dir = self.get("OUTPUTS", "LOCAL_OUTPUT_PATH", fallback="")
        if self.outputs_dir and self.outputs_dir != config_output_dir:
            logger.warning(
                "Passed local output directory is different than the config file. Saving in %s",
                self.outputs_dir,
            )
            return self.outputs_dir
        return config_output_dir or self.outputs_dir or ""

    @property
    def prediction_folder_name(self) -> str:
        return self.get("OUTPUTS", "PREDICTION_FOLDER_NAME")
