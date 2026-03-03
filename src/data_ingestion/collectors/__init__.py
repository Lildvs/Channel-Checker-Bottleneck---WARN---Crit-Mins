"""Data collectors for various government and alternative data sources."""

from src.data_ingestion.collectors.aircraft_parts_collector import (
    AircraftPartsCollector,
    get_aircraft_parts_collector,
)
from src.data_ingestion.collectors.bea_io_collector import (
    BEAIOCollector,
    get_bea_io_collector,
)
from src.data_ingestion.collectors.bea_io_file_collector import (
    BEAIOFileCollector,
    get_bea_io_file_collector,
)
from src.data_ingestion.collectors.bls_collector import BLSCollector
from src.data_ingestion.collectors.census_collector import CensusCollector
from src.data_ingestion.collectors.commodity_inventory_collector import (
    CommodityInventoryCollector,
    get_commodity_inventory_collector,
)
from src.data_ingestion.collectors.commodity_inventory_file_collector import (
    CommodityInventoryFileCollector,
    get_commodity_inventory_file_collector,
    get_lme_collector,
    get_comex_collector,
)
from src.data_ingestion.collectors.critical_minerals_collector import (
    CriticalMineralsCollector,
    get_critical_minerals_collector,
)
from src.data_ingestion.collectors.eia_collector import EIACollector
from src.data_ingestion.collectors.energy_flows_collector import (
    EnergyFlowsCollector,
    get_energy_flows_collector,
)
from src.data_ingestion.collectors.steel_production_collector import (
    SteelProductionCollector,
    get_steel_production_collector,
)
from src.data_ingestion.collectors.fred_collector import FREDCollector
from src.data_ingestion.collectors.gdelt_collector import GDELTCollector
from src.data_ingestion.collectors.google_trends_collector import GoogleTrendsCollector
from src.data_ingestion.collectors.research_collector import ResearchCollector
from src.data_ingestion.collectors.sec_edgar_collector import (
    SECEdgarCollector,
    get_sec_edgar_collector,
)
from src.data_ingestion.collectors.ship_manufacturing_collector import (
    ShipManufacturingCollector,
    get_ship_manufacturing_collector,
)
from src.data_ingestion.collectors.shipping_collector import (
    ShippingDataCollector,
    get_shipping_collector,
)
from src.data_ingestion.collectors.usa_trade_collector import USATradeCollector
from src.data_ingestion.collectors.va_collector import VAHealthcareCollector
from src.data_ingestion.collectors.vehicle_production_collector import (
    VehicleProductionCollector,
    get_vehicle_production_collector,
)
from src.data_ingestion.collectors.warn_collector import WARNCollector
from src.data_ingestion.collectors.nfib_collector import NFIBCollector
from src.data_ingestion.collectors.port_congestion_collector import PortCongestionCollector
from src.data_ingestion.collectors.bnpl_sec_collector import BNPLSECCollector

__all__ = [
    "BEAIOCollector",
    "get_bea_io_collector",
    "BEAIOFileCollector",
    "get_bea_io_file_collector",
    "BLSCollector",
    "CensusCollector",
    "EIACollector",
    "FREDCollector",
    "GDELTCollector",
    "GoogleTrendsCollector",
    "ResearchCollector",
    "USATradeCollector",
    "VAHealthcareCollector",
    "WARNCollector",
    "SECEdgarCollector",
    "get_sec_edgar_collector",
    "CommodityInventoryCollector",
    "get_commodity_inventory_collector",
    "CommodityInventoryFileCollector",
    "get_commodity_inventory_file_collector",
    "get_lme_collector",
    "get_comex_collector",
    "ShippingDataCollector",
    "get_shipping_collector",
    "CriticalMineralsCollector",
    "get_critical_minerals_collector",
    "VehicleProductionCollector",
    "get_vehicle_production_collector",
    "ShipManufacturingCollector",
    "get_ship_manufacturing_collector",
    "AircraftPartsCollector",
    "get_aircraft_parts_collector",
    "EnergyFlowsCollector",
    "get_energy_flows_collector",
    "SteelProductionCollector",
    "get_steel_production_collector",
    "NFIBCollector",
    "PortCongestionCollector",
    "BNPLSECCollector",
]
