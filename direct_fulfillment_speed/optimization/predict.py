import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Union

import numpy as np

from direct_fulfillment_speed.entities.nodes import ODS, ShipmentType, Warehouse
from direct_fulfillment_speed.entities.shipment import ShipmentClass, ShipmentInstance
from direct_fulfillment_speed.utils.config import ConfigManager
from direct_fulfillment_speed.utils.util import concat_strings

logger = logging.getLogger(__name__)


class Predict:
    """
    A class for predicting delivery times for Origin-Destination-Shipment method (ODS) combinations.

    This class handles the forecasting process, including the identification of sparse and
    non-sparse ODSs, processing of non-sparse ODSs, and extrapolation for sparse ODSs using
    a weighted similarity score approach.

    Attributes:
        config (ConfigManager): Configuration manager object.
        ship_object (ShipmentClass): Object of ShipmentClass.
        A dictionary of shipment groups.
        forecasts (Dict[Union[ODS, Warehouse], Dict[float, float]]): Storage for forecast results.
        quantile_list (List[float]): List of quantiles to calculate.
        lambda_decay (float): Decay factor for time-based weighting.
    """

    def __init__(self, config: ConfigManager, shipments: ShipmentClass):
        self.config = config
        self.ship_object = shipments
        self.forecasts: Dict[Union[ODS, Warehouse], Dict[float, float]] = {}
        self.quantile_list = self.config.quantile_list
        self.lambda_decay = self.config.lambda_decay
        self.similarity_finder = SimilarityFinder(shipments)

    @staticmethod
    def calculate_time_decay_weights(
        dates: List[datetime], lambda_decay: float = 0.1
    ) -> List[float]:
        """
        Calculate time decay weights for a list of dates.

        Args:
            dates (List[datetime]): List of dates.
            lambda_decay (float): Decay factor for time-based weighting.

        Returns:
            List[float]: Calculated weights for the dates.
        """
        most_recent_date = max(dates)
        dates_diff = np.array([(most_recent_date - date_obj).days for date_obj in dates])
        weights = lambda_decay * ((1 - lambda_decay) ** np.sqrt(dates_diff))
        if weights.sum() == 0:
            weights = np.ones(len(weights))
        return weights / weights.sum()

    def process_ods(self, ods_list: List[Union[ODS, Warehouse]]) -> None:
        """
        Process a list of ODSs and calculate their forecasts.

        Args:
            ods_list (List[Union[ODS, Warehouse]]): List of non-sparse ODSs to perform prediction on.
        """
        for ods in ods_list:
            shipments = self.ship_object.get_shipments_for_entity(ods)
            if shipments:
                self.forecasts[ods] = self.calculate_weighted_average(shipments)

    def extrapolate_sparse_ods(
        self, sparse_ods: List[ODS], non_sparse_ods: List[Union[ODS, Warehouse]]
    ) -> None:
        """
        Extrapolate predictions for sparse ODSs based on similar non-sparse ODSs.

        Args:
            sparse_ods (List[ODS]): List of sparse ODSs.
            non_sparse_ods (List[Union[ODS, Warehouse]]): List of non-sparse ODSs.
        """
        for sparse in sparse_ods:
            similar_non_sparse = self.similarity_finder.find_similar_non_sparse(
                sparse, [ods for ods in non_sparse_ods if isinstance(ods, ODS)]
            )
            if similar_non_sparse:
                self.forecasts[sparse] = self.get_estimated_distribution_for_sparse_ods(
                    similar_non_sparse
                )
            else:
                logger.warning(f"Unable to extrapolate forecast for sparse ODS: {sparse}")

    def get_estimated_distribution_for_sparse_ods(
        self, similar_non_sparse: List[Tuple[ODS, float]]
    ) -> Dict[float, float]:
        """
        Estimate the weighted estimated pad distribution for a sparse ODS based
        on similar non-sparse ODSs.

        Args:
            similar_non_sparse (List[Tuple[ODS, float]]): List of tuples containing non-sparse
            ODSs and their similarity scores.

        Returns:
            Dict[float, int]: Weighted forecast for the sparse ODS with integer values.
        """
        if not similar_non_sparse:
            return {}

        # Get the highest score
        highest_score = max(score for _, score in similar_non_sparse)
        highest_similarity_ods = [
            ods for ods, score in similar_non_sparse if score == highest_score
        ]

        total_similarity = sum(highest_score for _ in highest_similarity_ods)
        if total_similarity == 0:
            return {}

        weighted_forecast: Dict[float, float] = {}
        for non_sparse in highest_similarity_ods:
            if non_sparse in self.forecasts:
                for quantile, value in self.forecasts[non_sparse].items():
                    if quantile not in weighted_forecast:
                        weighted_forecast[quantile] = 0
                    weighted_forecast[quantile] += (highest_score / total_similarity) * value

        # Round the weighted forecasts to the nearest integer
        integer_weighted_forecast = {
            quantile: np.round(value) for quantile, value in weighted_forecast.items()
        }
        return integer_weighted_forecast

    def calculate_weighted_average(self, shipments: List[ShipmentInstance]) -> Dict[float, float]:
        """
        Calculate the weighted average for a group of shipments.

        Args:
            shipments (List[ShipmentInstance]): List of shipments.

        Returns:
            Dict[float, float]: Dictionary of quantile forecasts.
        """
        valid_dates = []
        valid_c2d_days_values = []

        for shipment in shipments:
            if shipment.get_order_date and shipment.c2p_days_unpadded is not None:
                valid_dates.append(shipment.get_order_date)
                valid_c2d_days_values.append(float(shipment.c2d_c2p_unpadded_gap))

        if valid_dates and valid_c2d_days_values:
            weights = self.calculate_time_decay_weights(valid_dates, lambda_decay=self.lambda_decay)
            return self.adjust_quantiles(valid_c2d_days_values, weights, self.quantile_list)
        else:
            logger.error("No valid data for weighted average calculation.")
            return {}

    def adjust_quantiles(
        self, values: List[float], weights: List[float], quantiles: List[float]
    ) -> Dict[float, float]:
        """
        Adjust the calculation for multiple quantiles, ensuring that 0 is included if missing.

        Args:
            values (List[float]): List of values.
            weights (List[float]): List of weights.
            quantiles (List[float]): List of quantiles to calculate.

        Returns:
            Dict[float, float]: Dictionary of adjusted quantile forecasts.
        """

        # Convert values, weights, and quantiles to NumPy arrays
        values = np.array(values, dtype=np.float64)
        weights = np.array(weights, dtype=np.float64)
        quantiles = (
            np.array(quantiles, dtype=np.float64) / 100.0
        )  # Convert percentages to proportions

        # Sort the data and weights based on the values
        sorter = np.argsort(values)
        values_sorted = values[sorter]
        weights_sorted = weights[sorter]

        # Compute the cumulative sum of weights
        cumulative_weights = np.cumsum(weights_sorted)
        total_weight = cumulative_weights[-1]

        # Normalize cumulative weights to range [0, 1]
        normalized_weights = cumulative_weights / total_weight

        # Use the weighted rank method
        quantile_values = []
        for q in quantiles:
            # Find the first index where normalized_weights exceeds the quantile
            idx = np.searchsorted(normalized_weights, q)
            if idx == len(values_sorted):
                quantile_values.append(values_sorted[-1])
            else:
                quantile_values.append(values_sorted[idx])

        # Create a dictionary of quantile results
        quantile_forecasts = {q * 100: v for q, v in zip(quantiles, quantile_values)}

        # Check if zero should be included in quantile forecasts
        if not any(v == 0 for v in quantile_forecasts.values()) and any(w == 0 for w in values):
            quantile_forecasts = self.insert_zero_into_quantiles(quantile_forecasts)

        # Ensure unique quantile values
        unique_values = set(quantile_forecasts.values())
        for value in unique_values:
            duplicates = {q: v for q, v in quantile_forecasts.items() if v == value}
            if len(duplicates) > 1:
                highest_quantile = max(duplicates.keys())
                for q in list(duplicates):
                    if q != highest_quantile:
                        del quantile_forecasts[q]

        return quantile_forecasts

    @staticmethod
    def insert_zero_into_quantiles(quantile_forecasts: Dict[float, float]) -> Dict[float, float]:
        """
        Insert zero into the quantile forecasts if it's not already present.

        Args:
            quantile_forecasts (Dict[float, float]): Dictionary of quantile forecasts.

        Returns:
            Dict[float, float]: Updated dictionary of quantile forecasts with zero included.
        """
        sorted_quantiles = sorted(quantile_forecasts.items(), key=lambda x: x[1])
        zero_inserted = False

        for i, (q, v) in enumerate(sorted_quantiles):
            if v > 0:
                zero_quantile = ((sorted_quantiles[i - 1][0] + q) // 2) if i > 0 else q // 2
                quantile_forecasts[zero_quantile] = 0
                zero_inserted = True
                break

        if not zero_inserted and sorted_quantiles:
            highest_quantile = max(quantile_forecasts.keys())
            quantile_forecasts[highest_quantile + 1] = 0

        return quantile_forecasts

    def perform_forecasts(self) -> None:
        """
        Perform the forecasting process for all ODSs.
        """
        sparse_ods, non_sparse_ods = self.ship_object.identify_sparse_ods()
        self.process_ods(non_sparse_ods)
        self.extrapolate_sparse_ods(sparse_ods, non_sparse_ods)

    @property
    def get_forecasts(self) -> Dict[Union[ODS, Warehouse], Dict[float, float]]:
        """
        Get the calculated forecasts.

        Returns:
            Dict[Union[ODS, Warehouse], Dict[float, float]]: The calculated forecasts.
        """
        return self.forecasts


class SimilarityFinder:
    """
    A class to find similar ODSs to the non-sparse ones based on some attributes.

    Attributes:
        ship_object (ShipmentClass): Object of ShipmentClass.
        shipment_groups (Dict[str, Dict[Union[ODS, Warehouse], List[ShipmentInstance]]]):
        A dictionary of shipment groups.
    """

    def __init__(self, shipments: ShipmentClass):
        self.ship_object = shipments
        self.shipment_groups: Dict[
            str, Dict[Union[ODS, Warehouse], List[ShipmentInstance]]
        ] = shipments.shipment_groups
        self.similarity_cache: Dict[ShipmentType, Dict[str, List[Tuple[ODS, float]]]] = defaultdict(
            dict
        )

    @staticmethod
    def calculate_similarity_score(sparse: ODS, non_sparse: ODS) -> float:
        """
        Calculate a similarity score between a sparse and non-sparse ODS.

        This method calculates a weighted similarity score based on several key attributes:
        - Origin warehouse: If the origin warehouses are identical, it will give us the highest
         similarity score.
        - Distance zone: A match in distance zone contributes to the score when origins match.
        - GL group: Matching GL groups add to the similarity score when origins differ.
        - Zip code proximity: The score is adjusted by the difference in destination zip codes,
        ensuring nearby matches are favored.

        It prioritizes exact matches in origin and distance zone,
        and considers combinations of GL group and distance zone when origins differ.

        Args:
            sparse (ODS): The sparse ODS to compare.
            non_sparse (ODS): The non-sparse ODS to compare against.

        Returns:
            float: A similarity score between 0 and 100.
        """
        weights = {
            "origin": 45,
            "primary_gl": 25,
            "distance_zone": 25,
            "zip_diff": 5,
        }

        score = 0

        # Check if origin warehouses match
        if sparse.origin == non_sparse.origin:
            score += weights["origin"]
            # Check for compatible GL group
            if sparse.primary_gl == non_sparse.primary_gl:
                score += weights["primary_gl"]
            # Check for compatible distance zones
            if sparse.distance_zone == non_sparse.distance_zone:
                score += weights["distance_zone"]
        else:
            # Check if GL group and distance zone match
            if (
                sparse.primary_gl == non_sparse.primary_gl
                and sparse.distance_zone == non_sparse.distance_zone
            ):
                score += weights["primary_gl"] + weights["distance_zone"]

        # Adjust score based on zip code difference
        zip_diff = abs(int(sparse.dest.dest_zip3) - int(non_sparse.dest.dest_zip3))
        score += max(0, weights["zip_diff"] - zip_diff)

        return score

    def find_similar_non_sparse(
        self, sparse: ODS, non_sparse_ods: List[ODS]
    ) -> List[Tuple[ODS, float]]:
        """
        Find the most similar non-sparse ODSs for a given sparse ODS, considering compatible shipment types
        and caching results for attribute combinations.

        This method checks the cache for precomputed results based on combinations of 'primary_gl', 'distance_zone',
        and 'origin'. If a cache hit is found, it returns the cached results. If no cache hit is found, it computes
        the similarities, sorts the results, caches the best matches, and returns them.

        Args:
            sparse (ODS): The sparse ODS to find a match for.
            non_sparse_ods (List[ODS]): List of non-sparse ODSs to search.

        Returns:
            List[Tuple[ODS, float]]: List of tuples with the most similar non-sparse ODS and their similarity scores.
        """
        if not non_sparse_ods:
            return []

        # Define the attributes to consider
        attributes = ["origin", "primary_gl", "distance_zone"]

        # Determine the shipment type key
        primary_key = sparse.carrier.shipment_type

        # Create a unique key for the sparse ODS based on its attributes
        secondary_key = concat_strings("_", *(getattr(sparse, attr) for attr in attributes))

        # Check cache
        if secondary_key in self.similarity_cache[primary_key]:
            return self.similarity_cache[primary_key][secondary_key]

        # Filter non_sparse_ods based on compatible shipment types
        compatible_non_sparse_ods = [
            ods
            for ods in non_sparse_ods
            if (
                primary_key in [ShipmentType.UPS_AIR, ShipmentType.UPS_GROUND]
                and ods.carrier.shipment_type in [ShipmentType.UPS_AIR, ShipmentType.UPS_GROUND]
            )
            and sparse.carrier.shipment_type == ods.carrier.shipment_type
        ]

        if not compatible_non_sparse_ods:
            return []

        # Calculate similarities
        similarities = [
            (ods, self.calculate_similarity_score(sparse, ods)) for ods in compatible_non_sparse_ods
        ]
        similarities.sort(key=lambda x: x[1], reverse=True)

        # Get the best matches
        best_score = similarities[0][1]
        best_matches = [item for item in similarities if item[1] == best_score]

        if len(best_matches) > 1:
            best_matches.sort(
                key=lambda x: abs(int(sparse.dest.dest_zip3) - int(x[0].dest.dest_zip3))
            )

        # Store the result in the cache
        self.similarity_cache[primary_key][secondary_key] = best_matches

        return best_matches
