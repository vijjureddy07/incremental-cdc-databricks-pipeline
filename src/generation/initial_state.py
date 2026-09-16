"""Deterministic generator for initial B2B SaaS source OLTP state."""

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config.settings import (
    ACCOUNT_TIERS,
    COUNTRIES,
    INDUSTRIES,
    INVOICE_STATUSES,
    PAYMENT_STATUSES,
    SCALE_PROFILES,
    SUBSCRIPTION_PLANS,
    SUBSCRIPTION_STATUSES,
    ScaleConfig,
)


@dataclass
class InitialSourceState:
    """In-memory representation of initial OLTP system state."""

    accounts: List[Dict[str, Any]]
    subscriptions: List[Dict[str, Any]]
    invoices: List[Dict[str, Any]]
    payments: List[Dict[str, Any]]
    highest_initial_sequence: int
    scale: str
    seed: int


def generate_initial_state(
    scale: str = "tiny",
    seed: int = 42,
    output_dir: Optional[Path] = None,
) -> InitialSourceState:
    """Generate deterministic initial state for accounts, subscriptions, invoices, payments.

    Guarantees: Same (scale, seed) -> identical state and sequence order.
    """
    if scale not in SCALE_PROFILES:
        raise ValueError(f"Unknown scale: {scale}. Choose from {list(SCALE_PROFILES.keys())}")

    config: ScaleConfig = SCALE_PROFILES[scale]
    rng = random.Random(seed)

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    current_seq = 0

    # 1. Accounts
    accounts: List[Dict[str, Any]] = []
    for i in range(1, config.num_accounts + 1):
        current_seq += 1
        created_time = base_time + timedelta(minutes=i * 5)
        acc = {
            "account_id": f"ACC-{i:06d}",
            "company_name": f"Enterprise-{i} {rng.choice(INDUSTRIES)} Inc",
            "industry": rng.choice(INDUSTRIES),
            "country": rng.choice(COUNTRIES),
            "plan_tier": rng.choice(ACCOUNT_TIERS),
            "source_sequence": current_seq,
            "created_at": created_time.isoformat(),
            "updated_at": created_time.isoformat(),
        }
        accounts.append(acc)

    # 2. Subscriptions
    subscriptions: List[Dict[str, Any]] = []
    for i in range(1, config.num_subscriptions + 1):
        current_seq += 1
        parent_account = accounts[(i - 1) % len(accounts)]
        created_time = datetime.fromisoformat(parent_account["created_at"]) + timedelta(minutes=10)
        renewal_date = (created_time + timedelta(days=365)).strftime("%Y-%m-%d")
        sub = {
            "subscription_id": f"SUB-{i:06d}",
            "account_id": parent_account["account_id"],
            "plan": rng.choice(SUBSCRIPTION_PLANS),
            "status": rng.choice(SUBSCRIPTION_STATUSES),
            "monthly_amount": float(rng.choice([49.0, 199.0, 499.0, 1200.0])),
            "renewal_date": renewal_date,
            "source_sequence": current_seq,
            "created_at": created_time.isoformat(),
            "updated_at": created_time.isoformat(),
        }
        subscriptions.append(sub)

    # 3. Invoices
    invoices: List[Dict[str, Any]] = []
    for i in range(1, config.num_invoices + 1):
        current_seq += 1
        sub = subscriptions[(i - 1) % len(subscriptions)]
        created_time = datetime.fromisoformat(sub["created_at"]) + timedelta(days=(i % 30) + 1)
        due_date = (created_time + timedelta(days=30)).strftime("%Y-%m-%d")
        inv = {
            "invoice_id": f"INV-{i:06d}",
            "account_id": sub["account_id"],
            "subscription_id": sub["subscription_id"],
            "amount": sub["monthly_amount"],
            "status": rng.choice(INVOICE_STATUSES),
            "due_date": due_date,
            "source_sequence": current_seq,
            "created_at": created_time.isoformat(),
            "updated_at": created_time.isoformat(),
        }
        invoices.append(inv)

    # 4. Payments
    payments: List[Dict[str, Any]] = []
    for i in range(1, config.num_payments + 1):
        current_seq += 1
        inv = invoices[(i - 1) % len(invoices)]
        created_time = datetime.fromisoformat(inv["created_at"]) + timedelta(days=2)
        pmt = {
            "payment_id": f"PAY-{i:06d}",
            "invoice_id": inv["invoice_id"],
            "amount": inv["amount"],
            "payment_status": rng.choice(PAYMENT_STATUSES),
            "processor_ref": f"proc_tx_{rng.randint(1000000, 9999999)}",
            "source_sequence": current_seq,
            "created_at": created_time.isoformat(),
            "updated_at": created_time.isoformat(),
        }
        payments.append(pmt)

    state = InitialSourceState(
        accounts=accounts,
        subscriptions=subscriptions,
        invoices=invoices,
        payments=payments,
        highest_initial_sequence=current_seq,
        scale=scale,
        seed=seed,
    )

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        for table_name, records in [
            ("accounts", accounts),
            ("subscriptions", subscriptions),
            ("invoices", invoices),
            ("payments", payments),
        ]:
            table_path = output_dir / f"{table_name}.jsonl"
            with open(table_path, "w", encoding="utf-8") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")

        meta_path = output_dir / "snapshot_metadata.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "scale": scale,
                    "seed": seed,
                    "counts": {
                        "accounts": len(accounts),
                        "subscriptions": len(subscriptions),
                        "invoices": len(invoices),
                        "payments": len(payments),
                    },
                    "highest_initial_sequence": current_seq,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
                f,
                indent=2,
            )

    return state
