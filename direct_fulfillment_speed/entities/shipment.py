import logging
import statistics
from collections import defaultdict
from datetime import timedelta
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from direct_fulfillment_speed.entities.nodes import (
    ODS,
    CarrierType,
    ShipmentType,
    ShippingCarrier,
    Warehouse,
)
from direct_fulfillment_speed.utils import util
from direct_fulfillment_speed.utils.config import ConfigManager

logger = logging.getLogger()


class ShipmentDistance:
    DISTANCE_THRESHOLDS = {
        "short": 300,
        "medium": 1000,
        # 'long' is anything above medium
    }

    def __init__(self, distance_mi):
        self.distance_mi = self._parse_distance(distance_mi)

    @staticmethod
    def _parse_distance(distance_mi):
        if distance_mi is None or distance_mi == "":
            return None
        try:
            return int(distance_mi)
        except ValueError:
            return None

    @property
    def zone(self):
        if self.distance_mi is None:
            return "unknown"
        for zone, threshold in self.DISTANCE_THRESHOLDS.items():
            if self.distance_mi < threshold:
                return zone
        return "long"

    def __str__(self):
        return f"{self.distance_mi} miles" if self.distance_mi is not None else "Unknown distance"

    def __repr__(self):
        return f"ShipmentDistance({self.distance_mi})"


class ShipmentInstance:
    """
    The class that contains the infor of each shipment.
    """

    def __init__(
        self,
        region_id,
        marketplace_id,
        shipment_id,
        order_id,
        tracking_id,
        package_id,
        vendor,
        warehouse,
        carrier,
        destination,
        ship_method,
        order_datetime,
        of_datetime,
        is_fasttrack,
        c2exsd,
        c2p_days,
        c2p_days_unpadded,
        c2d_days,
        att_c2d_days,
        att_failed_pdd,
        no_att_scan,
        unpadded_pdd_date,
        tt_pad,
        gl_group,
        vendor_primary_gl_description,
        distance_mi,
    ):
        self.region_id = region_id
        self.marketplace_id = marketplace_id
        self.shipment_id = shipment_id
        self.order_id = order_id
        self.tracking_id = tracking_id
        self.package_id = package_id
        self.vendor = vendor
        self.warehouse = warehouse
        self.carrier = carrier
        self.destination = destination
        self.ship_method = ship_method
        self._order_datetime_str = order_datetime
        self._of_datetime_str = of_datetime
        self.is_fasttrack = is_fasttrack
        self.c2exsd = c2exsd
        self.c2p_days = c2p_days
        self.c2p_days_unpadded = float(c2p_days_unpadded)
        self.c2d_days = float(c2d_days)
        self.att_c2d_days = att_c2d_days
        self.att_failed_pdd = att_failed_pdd
        self.no_att_scan = no_att_scan
        self.unpadded_pdd_date = unpadded_pdd_date
        self.tt_pad = tt_pad
        self.gl_group = gl_group
        self.primary_gl = vendor_primary_gl_description
        self.c2d_c2p_unpadded_gap = self.c2d_days - self.c2p_days_unpadded
        self.shipment_type = carrier.shipment_type
        self.distance = ShipmentDistance(distance_mi)

    @property
    def order_datetime(self):
        return util.convert_time_str_to_dt_object(self._order_datetime_str)

    @property
    def distance_to_zip3(self):
        return self.distance.distance_mi

    @property
    def distance_zone(self):
        return self.distance.zone

    @property
    def of_datetime(self):
        if pd.isna(self._of_datetime_str):
            return None
        return util.convert_time_str_to_dt_object(self._of_datetime_str)

    @property
    def get_order_date(self):
        return self.of_datetime if self.of_datetime is not None else self.order_datetime

    def __str__(self):
        """
        Print the instance. This is mostly for debugging purposes.

        Returns:
            str: A string representation of the instance.
        """
        return f"{(self.order_id, self.shipment_id, self.warehouse, self.carrier, self.destination.dest_zip3)}"

    def __repr__(self):
        """Print the instance"""
        return f"{(self.shipment_id, self.warehouse, self.carrier, self.destination.dest_zip3)}"

    def __eq__(self, other):
        if isinstance(other, ShipmentInstance):
            return self.hash_member == other.hash_member
        return False

    def __hash__(self):
        return hash(self.hash_member)

    @property
    def hash_member(self):
        """Use order, ship, track IDs, vendor, warehouse, etc. to compare shipments."""
        return (
            self.order_id,
            self.tracking_id,
            self.shipment_id,
            self.vendor,
            self.warehouse,
            self.destination,
            self.carrier,
        )


class ShipmentClass:
    def __init__(self, config: ConfigManager) -> None:
        self.shipment_groups: Dict[str, Dict[Union[ODS, Warehouse], List[ShipmentInstance]]] = {
            CarrierType.ALL.name: defaultdict(list),
            CarrierType.THIRD_PARTY.name: defaultdict(list),
            ShippingCarrier.SWA.name: defaultdict(list),
            CarrierType.OTHERS.name: defaultdict(list),
        }
        self.config = config
        self.sparse_groups: Dict[
            str, Dict[Union[ODS, Warehouse], List[ShipmentInstance]]
        ] = defaultdict(dict)
        self.original_to_clustered_ods: Dict[ODS, List[ODS]] = defaultdict(list)

        self.sparsity_threshold = self.config.min_ods_count

        # Convert the model test end date and ensure it is not None
        self.model_test_end_date = util.convert_time_str_to_dt_object(
            self.config.get_model_test_end_date
        )
        if self.model_test_end_date is None:
            raise ValueError("Test end date is not configured or is None.")

        # Find the most recent of_datetime
        # TODO: For now, get the unpadded_DEA for T4W (3weeks). Need to check it out again.
        self.most_recent_shipment = (self.model_test_end_date - timedelta(days=28)).date()
        if self.most_recent_shipment is None:
            raise ValueError("No date found for most recent date. Check config file.")

    def add_shipment(self, shipment_instance: ShipmentInstance) -> None:
        """
        Adds a shipment instance to the relevant group based on the carrier.

          This method checks the carrier of the given shipment instance and,
          if it's one of the specified carriers (SWA, UPS, AMXL), adds it to
          the appropriate group ('ALL', 'THIRD_PARTY', or 'SWA'). Shipments from
          other carriers are not processed and are added to OTHERS group.

          TODO: Re-evaluate the carrier filter. This method currently only processes
          shipments from SWA, and UPS carriers. Consider whether shipments
          from other carriers should also be included or processed differently.

        Args:
            shipment_instance: Object of shipment instance to be added.

        Returns:
            None
        """
        self.shipment_groups[CarrierType.ALL.name][shipment_instance.warehouse].append(
            shipment_instance
        )
        if shipment_instance.carrier.carrier in (
            ShippingCarrier.SWA.name,
            ShippingCarrier.UPS.name,
        ):
            group, key = self._determine_group_and_key_for_shipment(shipment_instance)
            self.shipment_groups[group][key].append(shipment_instance)
        else:
            self.shipment_groups[CarrierType.OTHERS.name][shipment_instance.warehouse].append(
                shipment_instance
            )

    @staticmethod
    def _determine_group_and_key_for_shipment(
        shipment_instance: ShipmentInstance,
    ) -> Tuple[str, Union[ODS, Warehouse]]:
        """
        Identify 1P vs. 3P shipments and carries.
        Args:
            shipment_instance: Object of shipment instance to be added.

        Returns:
            String contains the name of teh carrier and node type i.e., ODS or SWA.

        """
        if shipment_instance.carrier.carrier == ShippingCarrier.SWA.name:
            return ShippingCarrier.SWA.name, shipment_instance.warehouse
        else:
            return CarrierType.THIRD_PARTY.name, ODS(
                origin=shipment_instance.warehouse,
                carrier=shipment_instance.carrier,
                dest=shipment_instance.destination,
                shipment_method=shipment_instance.ship_method,
                distance_zone=shipment_instance.distance_zone,
            )

    def calculate_cumulative_ship_percentages(self) -> Dict[Union[ODS, Warehouse], float]:
        """
        Calculate shipment percentages across all relevant groups cumulatively.
        Returns:
            A dict of calculate shipment percentages for each ODS/Warehouse/ Carrier.

        """
        relevant_groups = [CarrierType.THIRD_PARTY.name, ShippingCarrier.SWA.name]
        total_shipments = sum(
            self.total_number_shipments_by_group(group) for group in relevant_groups
        )
        cumulative_percentages = {
            key: len(shipments) / total_shipments * 100
            for group in relevant_groups
            for key, shipments in self.shipment_groups[group].items()
        }
        return cumulative_percentages

    def total_number_shipments_by_group(
        self, group: str, shipment_type: Optional[ShipmentType] = None
    ) -> int:
        """
        Return the total number of shipments by each group. For 3P, this method can
        count by AIR vs Grond total shipments.

        Args:
            group: name of 1P/3P carrier.
            shipment_type: Air/Ground/SWA.

        Returns:
            The total number of shipments in the specified group.
        """
        if group not in self.shipment_groups:
            raise ValueError(f"Group '{group}' not found in shipment groups.")

        if shipment_type:
            return sum(
                len([s for s in shipments if s.shipment_type == shipment_type])
                for shipments in self.shipment_groups[group].values()
            )
        else:
            return sum(len(shipments) for shipments in self.shipment_groups[group].values())

    def extract_ods_warehouse_metrics(self) -> None:
        """
        Calculate the unpadded DEA and padded DEA for each ODS or warehouse and add it to a dictionary.

        This method performs the following steps:
        1. Filters shipments for the most recent weeks.
        2. Groups the recent shipments by week.
        3. Calculates the DEA for each week.
        4. Uses the mean of the weekly DEAs to mitigate the effect of outliers.
        5. Calculates C2P metrics using the mean.

        Calculated Metrics:
            key.recent_unpadded_dea (float): The original unpadded DEA calculated for comparison.
            key.recent_dea (float): The original padded DEA calculated for comparison.
            key.recent_unpadded_c2p_days (float): The median unpadded click-to-promise days.
            key.recent_c2d_days (float): The median click-to-delivery days.
            key.recent_c2p_days (float): The median click-to-promise days.
            key.recent_ship_counts (int): The total number of recent shipments.
        """
        for group in [CarrierType.THIRD_PARTY.name, ShippingCarrier.SWA.name]:
            for key, shipments in self.shipment_groups[group].items():
                # Filter shipments for the most recent weeks
                recent_shipments = [
                    s for s in shipments if s.get_order_date.date() >= self.most_recent_shipment
                ]
                if not recent_shipments:
                    continue

                total_recent_shipments = len(recent_shipments)
                key.recent_ship_counts = total_recent_shipments

                # Group shipments by week
                weekly_shipments = defaultdict(list)
                for s in recent_shipments:
                    week = (s.get_order_date.date() - self.most_recent_shipment).days // 7
                    weekly_shipments[week].append(s)

                weekly_unpadded_deas = []
                weekly_padded_deas = []

                for week_shipments in weekly_shipments.values():
                    week_total = len(week_shipments)
                    if week_total == 0:
                        continue

                    week_failed_unpadded = sum(
                        1 for s in week_shipments if s.c2p_days_unpadded < s.c2d_days
                    )
                    week_failed_padded = sum(1 for s in week_shipments if s.c2p_days < s.c2d_days)

                    week_unpadded_dea = (week_total - week_failed_unpadded) / week_total
                    week_padded_dea = (week_total - week_failed_padded) / week_total

                    weekly_unpadded_deas.append(week_unpadded_dea)
                    weekly_padded_deas.append(week_padded_dea)

                if weekly_unpadded_deas and weekly_padded_deas:
                    key.recent_unpadded_dea = statistics.mean(weekly_unpadded_deas)
                    key.recent_dea = statistics.mean(weekly_padded_deas)

                # Calculate and store c2ps
                if recent_shipments:
                    key.recent_unpadded_c2p_days = statistics.mean(
                        s.c2p_days_unpadded for s in recent_shipments
                    )
                    key.recent_c2d_days = statistics.mean(s.c2d_days for s in recent_shipments)
                    key.recent_c2p_days = statistics.mean(s.c2p_days for s in recent_shipments)

    def update_shipment_counts(self) -> None:
        """
        Update the ship_count attribute for each ODS and Warehouse based on current shipments.
        Returns:
            None.
        """
        for group in [CarrierType.THIRD_PARTY.name, ShippingCarrier.SWA.name]:
            for key, shipments in self.shipment_groups[group].items():
                total_shipments = len(shipments)
                key.ship_count = total_shipments

    def get_shipments_for_entity(self, entity: Union[ODS, Warehouse]) -> List[ShipmentInstance]:
        """
         Retrieve the shipments for a given ODS or Warehouse.
        Args:
            entity: ODS or Warehouse objects.

        Returns:
            List of shipment instances for each ODS or Warehouse.
        """
        if isinstance(entity, ODS):
            return self.shipment_groups[CarrierType.THIRD_PARTY.name].get(entity, [])
        elif isinstance(entity, Warehouse):
            return self.shipment_groups[ShippingCarrier.SWA.name].get(entity, [])
        else:
            logger.warning(f"Unknown entity type for {entity}.")
            return []

    def identify_sparse_ods(self) -> Tuple[List[ODS], List[Union[ODS, Warehouse]]]:
        """
        Identify sparse and non-sparse ODSs/Warehouses based on the number of shipments.

        Returns:
            A tuple containing lists of sparse and non-sparse ODSs.
        """
        sparse_ods: List[ODS] = []
        non_sparse_ods: List[Union[ODS, Warehouse]] = []
        total_shipments = 0
        sparse_shipments = 0

        for group in [CarrierType.THIRD_PARTY.name, ShippingCarrier.SWA.name]:
            for key, shipments in self.shipment_groups[group].items():
                shipment_count = key.ship_count
                total_shipments += shipment_count
                if shipment_count < self.sparsity_threshold:
                    if isinstance(key, ODS):
                        key.is_sparse = True
                        sparse_ods.append(key)
                    sparse_shipments += shipment_count
                else:
                    non_sparse_ods.append(key)
        return sparse_ods, non_sparse_ods
