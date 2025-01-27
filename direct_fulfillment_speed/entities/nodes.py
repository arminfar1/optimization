import enum
from collections import defaultdict
from typing import Dict, List, Set

from direct_fulfillment_speed.utils import util


class CarrierType(enum.Enum):
    FIRST_PARTY = "1P"
    THIRD_PARTY = "3P"
    ALL = "All"  # All shipment types
    OTHERS = "Others"


class ShipmentType(enum.Enum):
    UPS_AIR = "UPS_AIR"
    UPS_GROUND = "UPS_GROUND"
    SWA = "SWA"
    OTHER = "OTHER"

    @classmethod
    def from_ship_method(cls, ship_method: str):
        """Class method to determine the shipment type based on the ship method."""
        air_methods = {"UPS_NEXT_DAY", "UPS_2ND_DAY", "UPS_3_DAY"}
        third_party_ground_methods = {"UPS_GROUND"}
        first_party_ground_methods = {"SWA"}

        ship_method_upper = ship_method.upper()
        if ship_method_upper in air_methods:
            return cls.UPS_AIR
        elif ship_method_upper in first_party_ground_methods:
            return cls.SWA
        elif ship_method_upper in third_party_ground_methods:
            return cls.UPS_GROUND
        else:
            return cls.OTHER


class ShippingCarrier(enum.Enum):
    SWA = ("SWA", CarrierType.FIRST_PARTY)
    AMXL = ("AMXL", CarrierType.FIRST_PARTY)
    UPS = ("UPS", CarrierType.THIRD_PARTY)

    @property
    def carrier_code(self) -> str:
        return self.value[0]

    @property
    def carrier_type(self) -> CarrierType:
        return self.value[1]

    @classmethod
    def from_carrier_code(cls, carrier_code: str) -> "ShippingCarrier":
        """
        Returns a ShippingCarrier enum based on the given carrier code.
        """
        carrier_code = carrier_code.upper()
        for carrier in cls:
            if carrier.carrier_code == carrier_code:
                return carrier
        raise ValueError(f"{carrier_code} is not a valid ShippingCarrier")


class VendorManager:
    """Manages vendor creation and ensures uniqueness."""

    def __init__(self):
        self._vendor_cache = {}

    def get_or_create_vendor(self, vendor_id: str, vendor_primary_gl: str) -> "Vendor":
        """Fetches an existing vendor or creates a new one if it doesn't exist."""
        if vendor_id not in self._vendor_cache:
            vendor = Vendor(vendor_id, vendor_primary_gl)
            self._vendor_cache[vendor_id] = vendor
        return self._vendor_cache[vendor_id]


class Vendor:
    def __init__(self, vendor_id: str, vendor_primary_gl: str):
        self.vendor_id: str = vendor_id
        self.vendor_primary_gl: str = vendor_primary_gl
        self.warehouses: Set["Warehouse"] = set()

    def add_warehouse(self, warehouse: "Warehouse"):
        """Add a warehouse to this vendor."""
        self.warehouses.add(warehouse)

    def __str__(self):
        """Print the instance"""
        return f"{self.vendor_id}"

    def __repr__(self):
        """Print the instance"""
        return f"{self.vendor_id}"

    def __eq__(self, other):
        if isinstance(other, Vendor):
            return self.hash_member == other.hash_member
        return False

    def __hash__(self):
        return hash(self.hash_member)

    @property
    def hash_member(self):
        return (self.vendor_id,)


class Warehouse:
    def __init__(self, vendor: Vendor, warehouse_id: str, zip5: str):
        self.vendor = vendor
        self.warehouse_id: str = warehouse_id
        self.warehouse_zip5: str = util.to_zip5(zip5)
        self.warehouse_zip3: str = util.to_zip3(zip5)
        self.recent_unpadded_dea: float = -1.0
        self.recent_dea: float = -1.0
        self.recent_unpadded_c2p_days: float = -1.0
        self.recent_c2p_days: float = -1.0
        self.recent_c2d_days: float = -1.0
        self.ship_count: int = 0
        self.ship_count_before_clustering = 0
        self.use_zip3: bool = False
        self.recent_ship_counts: int = 0
        vendor.add_warehouse(self)

    @property
    def primary_gl(self):
        return self.vendor.vendor_primary_gl

    def __str__(self):
        """Print the instance"""
        return f"{self.warehouse_id}"

    def __repr__(self):
        """Print the instance"""
        return f"{self.warehouse_id}"

    def __eq__(self, other):
        if isinstance(other, Warehouse):
            return self.hash_member == other.hash_member
        return False

    def __hash__(self):
        return hash(self.hash_member)

    @property
    def hash_member(self):
        return (
            self.warehouse_id,
            self.warehouse_zip5,
        )


class Carrier:
    def __init__(self, carrier_name: str, ship_method_orig: str, ship_method: str):
        self.carrier: str = carrier_name
        self.ship_method_orig: str = ship_method_orig
        self.ship_method: str = ship_method
        self.carrier_type: CarrierType = self.determine_carrier_type(carrier_name)
        self.shipment_type: ShipmentType = ShipmentType.from_ship_method(ship_method)

    @staticmethod
    def determine_carrier_type(carrier_name: str) -> CarrierType:
        """Determine the carrier type based on the carrier name."""
        try:
            # Get the ShippingCarrier enum based on the carrier name
            shipping_carrier = ShippingCarrier.from_carrier_code(carrier_name)
            return shipping_carrier.carrier_type
        except ValueError:
            # Handle unknown carriers here, e.g., by logging or setting a default
            return CarrierType.THIRD_PARTY

    @property
    def type(self) -> CarrierType:
        """Property to access the carrier type."""
        return self.carrier_type

    def __str__(self):
        """Print the instance"""
        return f"Carrier: {self.carrier}, Ship Method: {self.ship_method}, Type: {self.shipment_type.name}"

    def __repr__(self):
        """Print the instance"""
        return f"{self.ship_method}"

    def __eq__(self, other):
        if isinstance(other, Carrier):
            return self.hash_member == other.hash_member
        return False

    def __hash__(self):
        return hash(self.hash_member)

    @property
    def hash_member(self):
        return (
            self.carrier,
            self.ship_method,
        )


class Destination:
    def __init__(self, zip5: str):
        self.dest_zip5: str = util.to_zip5(zip5)
        self.dest_zip3: str = util.to_zip3(self.dest_zip5)

    def __str__(self):
        """Print the instance"""
        return f"{self.dest_zip5}"

    def __repr__(self):
        """Print the instance"""
        return f"{self.dest_zip5}"

    def __eq__(self, other):
        if isinstance(other, Destination):
            return self.hash_member == other.hash_member
        return False

    def __hash__(self):
        return hash(self.hash_member)

    @property
    def hash_member(self):
        return (
            self.dest_zip5,
            self.dest_zip3,
        )


class ODS:
    """Warehouse/Zip3->Zip3->Ship-method"""

    def __init__(
        self,
        origin: Warehouse,
        carrier: Carrier,
        dest: Destination,
        shipment_method: str,
        use_zip3: bool = False,
        distance_zone: str = "",
    ):
        self.origin = origin
        self.carrier = carrier
        self.dest = dest
        self.ship_method = shipment_method
        self.is_sparse: bool = use_zip3
        self.distance_zone: str = distance_zone
        self.recent_unpadded_dea: float = -1.0
        self.recent_dea: float = -1.0
        self.recent_unpadded_c2p_days: float = -1.0
        self.recent_c2p_days: float = -1.0
        self.recent_c2d_days: float = -1.0
        self.ship_count: int = 0
        self.recent_ship_counts: int = 0

    @property
    def primary_gl(self):
        return self.origin.primary_gl

    def __str__(self):
        """Print the instance"""
        return f"{self.origin.warehouse_id}_{self.dest.dest_zip3}_{self.ship_method}"

    def __repr__(self):
        """Print the instance"""
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, ODS):
            return self.hash_member == other.hash_member
        return False

    def __hash__(self):
        return hash(self.hash_member)

    @property
    def hash_member(self):
        return self.origin.warehouse_id, self.dest.dest_zip3, self.ship_method


class Node:
    """Data object for class Nodes."""

    def __init__(self):
        self.vendors_warehouses: Dict[Vendor, Set[Warehouse]] = defaultdict(set)

    def add_warehouse(self, vendor: Vendor, warehouse: Warehouse):
        """Associate a warehouse with a vendor."""
        self.vendors_warehouses[vendor].add(warehouse)

    def get_all_vendors_warehouses(self, vendor: Vendor) -> List[Warehouse]:
        """Get all warehouse objects of a vendor."""
        return list(self.vendors_warehouses.get(vendor, []))
