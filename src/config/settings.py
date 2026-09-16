"""Pipeline configuration settings and constants."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Set

# Project Root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Default Paths
DEFAULT_SAMPLE_DATA_DIR = PROJECT_ROOT / "data" / "sample"
DEFAULT_CDC_DATA_DIR = PROJECT_ROOT / "data" / "generated" / "cdc"
DEFAULT_CHECKPOINT_DIR = PROJECT_ROOT / "state" / "checkpoints"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"
DEFAULT_VALID_DIR = DEFAULT_OUTPUT_DIR / "valid_events"
DEFAULT_QUARANTINE_DIR = DEFAULT_OUTPUT_DIR / "quarantine"
DEFAULT_LATE_DIR = DEFAULT_OUTPUT_DIR / "late_events"
DEFAULT_METRICS_DIR = DEFAULT_OUTPUT_DIR / "metrics"

# Java Home auto-detection for macOS Homebrew OpenJDK 17
DEFAULT_JAVA_HOME = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
if "JAVA_HOME" not in os.environ and os.path.exists(DEFAULT_JAVA_HOME):
    os.environ["JAVA_HOME"] = DEFAULT_JAVA_HOME

# Domain Entities
SUPPORTED_TABLES: Set[str] = {"accounts", "subscriptions", "invoices", "payments"}

# CDC Operation Convention
OPERATION_INSERT = "INSERT"
OPERATION_UPDATE = "UPDATE"
OPERATION_DELETE = "DELETE"
ALLOWED_OPERATIONS: Set[str] = {OPERATION_INSERT, OPERATION_UPDATE, OPERATION_DELETE}


# Domain Constants
INDUSTRIES = ["Technology", "Healthcare", "Finance", "Retail", "Manufacturing", "Education"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "AU", "JP"]
ACCOUNT_TIERS = ["STARTER", "GROWTH", "ENTERPRISE"]
SUBSCRIPTION_PLANS = ["BASIC", "PRO", "ENTERPRISE"]
SUBSCRIPTION_STATUSES = ["ACTIVE", "TRIAL", "PAST_DUE"]
INVOICE_STATUSES = ["ISSUED", "PAID", "VOID"]
PAYMENT_STATUSES = ["SUCCESS", "PENDING", "FAILED"]


@dataclass(frozen=True)
class ScaleConfig:
    """Scale configuration for initial snapshot and CDC batch sizes."""

    name: str
    num_accounts: int
    num_subscriptions: int
    num_invoices: int
    num_payments: int
    batch_event_target: int


SCALE_PROFILES: Dict[str, ScaleConfig] = {
    "tiny": ScaleConfig(
        name="tiny",
        num_accounts=50,
        num_subscriptions=100,
        num_invoices=250,
        num_payments=200,
        batch_event_target=40,
    ),
    "small": ScaleConfig(
        name="small",
        num_accounts=1000,
        num_subscriptions=2000,
        num_invoices=5000,
        num_payments=4000,
        batch_event_target=500,
    ),
    "standard": ScaleConfig(
        name="standard",
        num_accounts=10000,
        num_subscriptions=20000,
        num_invoices=50000,
        num_payments=40000,
        batch_event_target=2500,
    ),
}


@dataclass
class DefectConfig:
    """Configurable defect rates for intentional CDC change-feed defect injection."""

    duplicate_event_rate: float = 0.03
    out_of_order_rate: float = 0.03
    late_event_rate: float = 0.03
    unknown_operation_rate: float = 0.015
    missing_key_rate: float = 0.015
    malformed_payload_rate: float = 0.015
    invalid_sequence_rate: float = 0.01
    invalid_timestamp_rate: float = 0.01


@dataclass
class PipelineConfig:
    """Master pipeline configuration."""

    sample_dir: Path = DEFAULT_SAMPLE_DATA_DIR
    cdc_dir: Path = DEFAULT_CDC_DATA_DIR
    checkpoint_dir: Path = DEFAULT_CHECKPOINT_DIR
    valid_dir: Path = DEFAULT_VALID_DIR
    quarantine_dir: Path = DEFAULT_QUARANTINE_DIR
    late_dir: Path = DEFAULT_LATE_DIR
    metrics_dir: Path = DEFAULT_METRICS_DIR
    scale: str = "tiny"
    seed: int = 42
    defects: DefectConfig = field(default_factory=DefectConfig)

    def get_scale_config(self) -> ScaleConfig:
        if self.scale not in SCALE_PROFILES:
            raise ValueError(
                f"Unknown scale: '{self.scale}'. Available scales: {list(SCALE_PROFILES.keys())}"
            )
        return SCALE_PROFILES[self.scale]
