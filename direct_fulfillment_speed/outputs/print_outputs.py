import csv
import io
import json
import logging
import os
from typing import Dict, List, Tuple, Union

from direct_fulfillment_speed.entities.nodes import ODS, Warehouse
from direct_fulfillment_speed.optimization.predict import Predict
from direct_fulfillment_speed.optimization.speed_optimizer import Optimize
from direct_fulfillment_speed.utils import util
from direct_fulfillment_speed.utils.config import ConfigManager
from direct_fulfillment_speed.utils.s3 import upload_s3_file, write_s3_json

logger = logging.getLogger()


class ProcessOutputs:
    """Process the results and prepare outputs."""

    def __init__(self, config: ConfigManager, prediction: Predict, prob: Optimize):
        self.prob: Optimize = prob
        self.config = config
        self.s3_output_folder: str = config.s3_output_folder
        self.local_output_folder: str = config.local_output_folder
        self.epsilon: float = config.epsilon
        self.output_choice: str = config.output_choice
        logger.info("Getting average speed from the solution.")
        self.average_speed = self.prob.get_average_speed
        logger.info("Getting the met DEAs from the solution.")
        self.dea_constraints_lhs = self.prob.get_dea_constraints_lhs
        logger.info("Writing the output data.")
        tt_pad_data, utt_pad_data = self.segregate_data(self.prob.filtered_pads)
        self.save_data(tt_pad_data, "TTpad")
        self.save_data(utt_pad_data, "UTTpad")
        self.save_metadata(self.prob.solution)
        self.save_forecast_data(forecast_data=prediction.forecasts)

    def save_data(self, data: List[Dict], file_prefix: str):
        if data:
            timestamp = util.date_now(include_time=True)
            filename = f"{file_prefix}_{timestamp}.csv"
            local_file_path = os.path.join(self.local_output_folder, filename)

            if self.output_choice in ["local", "both"]:
                # Save locally
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                with open(local_file_path, "w", newline="", encoding="utf-8") as csvfile:
                    fieldnames = list(data[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in data:
                        writer.writerow(row)
                logger.info(f"Data saved locally at {local_file_path}")

            if self.output_choice in ["s3", "both"] and self.s3_output_folder.startswith("s3://"):
                # Upload to S3
                s3_path = os.path.join(self.s3_output_folder, filename)
                upload_s3_file(s3_path, local_file_path, is_file=True)
                logger.info(f"Uploaded {filename} to {s3_path}")

    @staticmethod
    def segregate_data(selected_pads) -> Tuple[List[Dict], List[Dict]]:
        """
        Separate the optimization output to two groups of UTT and TT-Pad.
        Args:
            selected_pads: Pads with values=1 from the optimizers.

        Returns:
            Two tuples one for TT-pad and one for UTT.

        """
        tt_pad_data: List[Dict] = []
        utt_pad_data: List[Dict] = []

        for key_tuple, decision_var_value in selected_pads.items():
            entity, pad, quantile = key_tuple[0], key_tuple[1], key_tuple[2]
            pad_str = util.formatted_days_to_hours(pad.replace("N", "-").replace("P", "."))
            recent_unpadded_dea = entity.recent_unpadded_dea
            recent_dea = entity.recent_dea
            recent_unpadded_c2p_days = entity.recent_unpadded_c2p_days
            recent_c2d_days = entity.recent_c2d_days
            recent_c2p_days = entity.recent_c2p_days
            effect_start_date = util.date_now(include_time=False)
            effect_end_date = util.add_days_to_date(effect_start_date, 30)

            if isinstance(entity, ODS):
                destination_zip = entity.dest.dest_zip3
                ship_method = entity.ship_method
                data_list = tt_pad_data
                primary_gl = entity.origin.vendor.vendor_primary_gl

                warehouse_id = entity.origin.warehouse_id
                data_dict = {
                    "Warehouse": warehouse_id,
                    "Destination ZIP": destination_zip,
                    "Primary GL": primary_gl,
                    "Ship Method": ship_method,
                    "TT Pad Value(hour)": pad_str,
                    "Avg. Recent Unpadded C2P (days)": recent_unpadded_c2p_days,
                    "Avg. Recent C2P (days)": recent_c2p_days,
                    "Avg. Recent C2D (days)": recent_c2d_days,
                    "Agg. Recent Unpadded DEA": recent_unpadded_dea,
                    "Agg. Recent DEA": recent_dea,
                    "Ship Count": entity.ship_count,
                    "Effective Start Date": effect_start_date,
                    "Effective End Date": effect_end_date,
                    "Is Sparse?": entity.is_sparse,
                }
                data_list.append(data_dict)

            else:  # SWA
                warehouse_id = entity.warehouse_id
                destination_zip = "SWA_DEST"  # Assuming a default destination for SWA
                ship_method = "SWA"
                data_list = utt_pad_data
                primary_gl = entity.vendor.vendor_primary_gl

                data_dict = {
                    "Warehouse": warehouse_id,
                    "Primary GL": primary_gl,
                    "Destination ZIP": destination_zip,
                    "Ship Method": ship_method,
                    "TT Pad Value(hour)": pad_str,
                    "Avg. Recent Unpadded C2P (days)": recent_unpadded_c2p_days,
                    "Avg. Recent C2P (days)": recent_c2p_days,
                    "Avg. Recent C2D (days)": recent_c2d_days,
                    "Agg. Recent Unpadded DEA": recent_unpadded_dea,
                    "Agg. Recent DEA": recent_dea,
                    "Ship Count": entity.ship_count,
                    "Effective Start Date": effect_start_date,
                    "Effective End Date": effect_end_date,
                }
                data_list.append(data_dict)

        return tt_pad_data, utt_pad_data

    def save_metadata(self, solution: Dict) -> None:
        """
        Write the optimization and model metadata to the output folder.
        Args:
            solution: Xpress Optimizer's outputs like objective value, constraints LHS, etc.

        Returns:
            None.

        """
        metadata = {
            "optimization_status": solution["optimization_status"],
            "objective_value": solution["objective_value"],
        }

        # Build metadata for DEA constraints
        for key, dea_value in self.dea_constraints_lhs.items():
            shipment_type, gl_group = key
            key_name = f"{shipment_type}"
            if gl_group:
                key_name += f"_{gl_group}"
            dea_key = f"{key_name} DEA"
            metadata[dea_key] = dea_value

        # Build metadata for average speeds
        for key, speed_value in self.average_speed.items():
            shipment_type, gl_group = key
            key_name = f"{shipment_type}"
            if gl_group:
                key_name += f"_{gl_group}"
            speed_key = f"{key_name} Speed"
            metadata[speed_key] = speed_value

        # Continue with saving metadata as before
        metadata_filename = f"model_metadata_{util.date_now(include_time=True)}.json"
        if self.output_choice in ["local", "both"]:
            local_file_path = os.path.join(self.local_output_folder, metadata_filename)
            with open(local_file_path, "w") as file:
                json.dump(metadata, file, indent=4)
            logger.info(f"Metadata saved to local path: {local_file_path}")

        if self.output_choice in ["s3", "both"] and self.s3_output_folder.startswith("s3://"):
            s3_path = f"{self.s3_output_folder}/metadata/{metadata_filename}"
            write_s3_json(s3_path, metadata)
            logger.info(f"Uploaded metadata to {s3_path}")

    def save_forecast_data(
        self, forecast_data: Dict[Union[ODS, Warehouse], Dict[float, float]]
    ) -> None:
        """
        Save forecasted values to a file and upload it to S3 or save locally.
        """
        file_name = f"forecast_data_{util.date_now(include_time=True)}.csv"
        file_content = io.StringIO()

        writer = csv.writer(file_content)
        writer.writerow(["ODS/Warehouse", "Quantile", "Value"])

        for location, quantile_values in forecast_data.items():
            for quantile, value in quantile_values.items():
                writer.writerow([location, quantile, value])

        file_content.seek(0)

        if self.output_choice in ["local", "both"]:
            file_path = os.path.join(self.local_output_folder, file_name)
            os.makedirs(
                self.local_output_folder, exist_ok=True
            )  # Ensure the local output folder exists
            with open(file_path, "w", newline="", encoding="utf-8") as file:
                file.write(file_content.getvalue())
            logger.info(f"Forecast data saved locally at {file_path}")

        if self.output_choice in ["s3", "both"] and self.s3_output_folder.startswith("s3://"):
            s3_path = f"{self.s3_output_folder}/forecast/{file_name}"
            upload_s3_file(s3_path, file_content.getvalue(), is_file=False)
            logger.info(f"Uploaded forecast data to {s3_path}")

        file_content.close()
