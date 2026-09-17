"""Deterministic CDC batch generator simulating OLTP change log streams with intentional defects."""

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import (
    ACCOUNT_TIERS,
    COUNTRIES,
    INDUSTRIES,
    INVOICE_STATUSES,
    PAYMENT_STATUSES,
    SCALE_PROFILES,
    SUBSCRIPTION_PLANS,
    SUBSCRIPTION_STATUSES,
    DefectConfig,
    ScaleConfig,
)
from src.generation.initial_state import InitialSourceState, generate_initial_state
from src.schemas.cdc_schema import (
    CURRENT_SCHEMA_VERSION,
    OPERATION_DELETE,
    OPERATION_INSERT,
    OPERATION_UPDATE,
    CDCEvent,
    generate_event_id,
)


@dataclass
class BatchManifest:
    """Metadata manifest emitted with each CDC batch."""

    batch_id: int
    created_at: str
    minimum_source_sequence: int
    maximum_source_sequence: int
    event_count: int
    table_event_counts: Dict[str, int]
    schema_versions: Dict[str, int]
    defects_injected: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "created_at": self.created_at,
            "minimum_source_sequence": self.minimum_source_sequence,
            "maximum_source_sequence": self.maximum_source_sequence,
            "event_count": self.event_count,
            "table_event_counts": self.table_event_counts,
            "schema_versions": self.schema_versions,
            "defects_injected": self.defects_injected,
        }


class CDCGenerator:
    """Generates sequential CDC batches from an initial OLTP state."""

    def __init__(
        self,
        initial_state: Optional[InitialSourceState] = None,
        scale: str = "tiny",
        seed: int = 42,
        defects: Optional[DefectConfig] = None,
        schema_transition_batch: Optional[int] = None,
    ):
        self.scale = scale
        self.seed = seed
        self.rng = random.Random(seed)
        self.defects = defects or DefectConfig()
        self.schema_transition_batch = schema_transition_batch
        self.scale_config: ScaleConfig = SCALE_PROFILES[scale]

        # Initialize or inherit state
        if initial_state is None:
            self.state = generate_initial_state(scale=scale, seed=seed)
        else:
            self.state = initial_state

        self.current_sequence = self.state.highest_initial_sequence
        self.base_time = datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.last_batch_id = 0
        self.held_events_for_out_of_order: List[Dict[str, Any]] = []

        # In-memory entity lookups
        self.accounts_by_id = {a["account_id"]: dict(a) for a in self.state.accounts}
        self.subscriptions_by_id = {s["subscription_id"]: dict(s) for s in self.state.subscriptions}
        self.invoices_by_id = {i["invoice_id"]: dict(i) for i in self.state.invoices}
        self.payments_by_id = {p["payment_id"]: dict(p) for p in self.state.payments}

        self.account_counter = len(self.accounts_by_id)
        self.sub_counter = len(self.subscriptions_by_id)
        self.inv_counter = len(self.invoices_by_id)
        self.pmt_counter = len(self.payments_by_id)

    def _next_seq(self) -> int:
        self.current_sequence += 1
        return self.current_sequence

    def generate_batch(
        self,
        batch_id: int,
        target_events: Optional[int] = None,
        output_dir: Optional[Path] = None,
    ) -> Tuple[Dict[str, List[Dict[str, Any]]], BatchManifest]:
        """Generate a single CDC batch across all tables with realistic changes and defects."""
        self.last_batch_id = batch_id
        target = target_events or self.scale_config.batch_event_target
        batch_time = self.base_time + timedelta(hours=batch_id * 6)
        batch_events_by_table: Dict[str, List[Dict[str, Any]]] = {
            "accounts": [],
            "subscriptions": [],
            "invoices": [],
            "payments": [],
        }
        defects_injected: List[str] = []

        # Release any previously held out-of-order events into this batch
        if self.held_events_for_out_of_order:
            for held_event in self.held_events_for_out_of_order:
                t = held_event["source_table"]
                held_event["batch_id"] = batch_id
                held_event["ingested_timestamp"] = batch_time.isoformat()
                batch_events_by_table[t].append(held_event)
                defects_injected.append(
                    f"Out-of-order delayed event {held_event['event_id']} (seq {held_event['source_sequence']}) released in batch {batch_id}"
                )
            self.held_events_for_out_of_order.clear()

        # Allocate events per entity roughly proportionally
        # 15% accounts, 35% subscriptions, 30% invoices, 20% payments
        target_accounts = max(2, int(target * 0.15))
        target_subscriptions = max(3, int(target * 0.35))
        target_invoices = max(2, int(target * 0.30))
        target_payments = max(2, int(target * 0.20))

        # 1. Accounts CDC
        self._generate_account_events(
            batch_id,
            batch_time,
            target_accounts,
            batch_events_by_table["accounts"],
            defects_injected,
        )

        # 2. Subscriptions CDC (Includes legitimate multiple updates to the same key)
        sub_schema_ver = (
            2
            if (
                self.schema_transition_batch is not None
                and batch_id >= self.schema_transition_batch
            )
            else CURRENT_SCHEMA_VERSION
        )
        self._generate_subscription_events(
            batch_id,
            batch_time,
            target_subscriptions,
            batch_events_by_table["subscriptions"],
            defects_injected,
            schema_version=sub_schema_ver,
        )

        # 3. Invoices CDC
        self._generate_invoice_events(
            batch_id,
            batch_time,
            target_invoices,
            batch_events_by_table["invoices"],
            defects_injected,
        )

        # 4. Payments CDC
        self._generate_payment_events(
            batch_id,
            batch_time,
            target_payments,
            batch_events_by_table["payments"],
            defects_injected,
        )

        # 5. Inject synthetic defect scenarios per batch
        self._inject_batch_defects(batch_id, batch_time, batch_events_by_table, defects_injected)

        # Flatten to calculate global sequence bounds and counts
        all_events = [e for events in batch_events_by_table.values() for e in events]
        valid_seqs = [
            e["source_sequence"]
            for e in all_events
            if isinstance(e.get("source_sequence"), int) and e.get("source_sequence", 0) > 0
        ]
        min_seq = min(valid_seqs) if valid_seqs else 0
        max_seq = max(valid_seqs) if valid_seqs else 0

        table_counts = {t: len(evs) for t, evs in batch_events_by_table.items()}
        table_schema_versions = {t: CURRENT_SCHEMA_VERSION for t in table_counts}
        if "subscriptions" in table_schema_versions:
            table_schema_versions["subscriptions"] = sub_schema_ver

        manifest = BatchManifest(
            batch_id=batch_id,
            created_at=batch_time.isoformat(),
            minimum_source_sequence=min_seq,
            maximum_source_sequence=max_seq,
            event_count=len(all_events),
            table_event_counts=table_counts,
            schema_versions=table_schema_versions,
            defects_injected=defects_injected,
        )

        if output_dir:
            # Layout: data/generated/cdc/batch_id=000001/
            batch_folder_name = f"batch_id={batch_id:06d}"
            batch_path = output_dir / batch_folder_name
            batch_path.mkdir(parents=True, exist_ok=True)

            for table_name, events in batch_events_by_table.items():
                file_path = batch_path / f"{table_name}.jsonl"
                with open(file_path, "w", encoding="utf-8") as f:
                    for ev in events:
                        f.write(json.dumps(ev) + "\n")

            manifest_path = batch_path / "manifest.json"
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest.to_dict(), f, indent=2)

        return batch_events_by_table, manifest

    def _generate_account_events(
        self,
        batch_id: int,
        batch_time: datetime,
        count: int,
        out_list: List[Dict[str, Any]],
        defects: List[str],
    ) -> None:
        for idx in range(count):
            op_roll = self.rng.random()
            event_time = (batch_time + timedelta(minutes=idx * 2)).isoformat()

            if op_roll < 0.25 or not self.accounts_by_id:
                # INSERT new account
                self.account_counter += 1
                acc_id = f"ACC-{self.account_counter:06d}"
                seq = self._next_seq()
                payload = {
                    "account_id": acc_id,
                    "company_name": f"Enterprise-{self.account_counter} {self.rng.choice(INDUSTRIES)} Inc",
                    "industry": self.rng.choice(INDUSTRIES),
                    "country": self.rng.choice(COUNTRIES),
                    "plan_tier": self.rng.choice(ACCOUNT_TIERS),
                }
                self.accounts_by_id[acc_id] = payload
                ev = CDCEvent.create(
                    source_table="accounts",
                    operation=OPERATION_INSERT,
                    business_key=acc_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=payload,
                )
                out_list.append(ev.to_dict())

            elif op_roll < 0.90:
                # UPDATE existing account
                acc_id = self.rng.choice(list(self.accounts_by_id.keys()))
                seq = self._next_seq()
                current_acc = self.accounts_by_id[acc_id]
                new_tier = self.rng.choice(ACCOUNT_TIERS)
                current_acc["plan_tier"] = new_tier
                ev = CDCEvent.create(
                    source_table="accounts",
                    operation=OPERATION_UPDATE,
                    business_key=acc_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=current_acc,
                )
                out_list.append(ev.to_dict())

            else:
                # DELETE account (closed / churned)
                acc_id = self.rng.choice(list(self.accounts_by_id.keys()))
                seq = self._next_seq()
                # For DELETE: reduced payload containing identifying keys (tombstone)
                delete_payload = {
                    "account_id": acc_id,
                    "deleted_reason": "Customer requested closure",
                }
                ev = CDCEvent.create(
                    source_table="accounts",
                    operation=OPERATION_DELETE,
                    business_key=acc_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=delete_payload,
                )
                out_list.append(ev.to_dict())
                del self.accounts_by_id[acc_id]

    def _generate_subscription_events(
        self,
        batch_id: int,
        batch_time: datetime,
        count: int,
        out_list: List[Dict[str, Any]],
        defects: List[str],
        schema_version: int = CURRENT_SCHEMA_VERSION,
    ) -> None:
        # Demonstrate MULTIPLE LEGITIMATE UPDATES TO THE SAME BUSINESS KEY
        # Pick one active subscription to receive 2 distinct sequential updates in this batch
        multi_update_sub_id: Optional[str] = None
        if self.subscriptions_by_id and count >= 2:
            multi_update_sub_id = self.rng.choice(list(self.subscriptions_by_id.keys()))

        for idx in range(count):
            event_time = (batch_time + timedelta(minutes=idx * 2 + 1)).isoformat()

            # Handle multi-update demonstration
            if multi_update_sub_id and idx in (0, 1):
                seq = self._next_seq()
                sub = self.subscriptions_by_id[multi_update_sub_id]
                new_plan = "PRO" if idx == 0 else "ENTERPRISE"
                sub["plan"] = new_plan
                sub["monthly_amount"] = 199.0 if idx == 0 else 499.0
                if schema_version == 2:
                    sub["billing_cycle"] = "ANNUAL"
                    sub["currency"] = "USD"
                ev = CDCEvent.create(
                    source_table="subscriptions",
                    operation=OPERATION_UPDATE,
                    business_key=multi_update_sub_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    schema_version=schema_version,
                    payload_dict=dict(sub),
                )
                out_list.append(ev.to_dict())
                continue

            op_roll = self.rng.random()
            if op_roll < 0.30 or not self.subscriptions_by_id:
                # INSERT subscription
                self.sub_counter += 1
                sub_id = f"SUB-{self.sub_counter:06d}"
                acc_id = (
                    self.rng.choice(list(self.accounts_by_id.keys()))
                    if self.accounts_by_id
                    else "ACC-000001"
                )
                seq = self._next_seq()
                payload = {
                    "subscription_id": sub_id,
                    "account_id": acc_id,
                    "plan": self.rng.choice(SUBSCRIPTION_PLANS),
                    "status": "ACTIVE",
                    "monthly_amount": float(self.rng.choice([49.0, 199.0, 499.0])),
                    "renewal_date": (batch_time + timedelta(days=365)).strftime("%Y-%m-%d"),
                }
                if schema_version == 2:
                    payload["billing_cycle"] = self.rng.choice(["MONTHLY", "ANNUAL"])
                    payload["currency"] = self.rng.choice(["USD", "EUR", "GBP", "INR"])
                self.subscriptions_by_id[sub_id] = payload
                ev = CDCEvent.create(
                    source_table="subscriptions",
                    operation=OPERATION_INSERT,
                    business_key=sub_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    schema_version=schema_version,
                    payload_dict=payload,
                )
                out_list.append(ev.to_dict())

            elif op_roll < 0.90:
                # UPDATE subscription
                sub_id = self.rng.choice(list(self.subscriptions_by_id.keys()))
                seq = self._next_seq()
                sub = self.subscriptions_by_id[sub_id]
                sub["status"] = self.rng.choice(SUBSCRIPTION_STATUSES)
                if schema_version == 2:
                    if "billing_cycle" not in sub:
                        sub["billing_cycle"] = self.rng.choice(["MONTHLY", "ANNUAL"])
                    if "currency" not in sub:
                        sub["currency"] = self.rng.choice(["USD", "EUR", "GBP", "INR"])
                    if self.rng.random() < 0.3:
                        sub["billing_cycle"] = (
                            "ANNUAL" if sub.get("billing_cycle") == "MONTHLY" else "MONTHLY"
                        )
                ev = CDCEvent.create(
                    source_table="subscriptions",
                    operation=OPERATION_UPDATE,
                    business_key=sub_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    schema_version=schema_version,
                    payload_dict=sub,
                )
                out_list.append(ev.to_dict())

            else:
                # DELETE subscription
                sub_id = self.rng.choice(list(self.subscriptions_by_id.keys()))
                seq = self._next_seq()
                delete_payload = {
                    "subscription_id": sub_id,
                    "cancellation_reason": "Churn",
                }
                ev = CDCEvent.create(
                    source_table="subscriptions",
                    operation=OPERATION_DELETE,
                    business_key=sub_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    schema_version=schema_version,
                    payload_dict=delete_payload,
                )
                out_list.append(ev.to_dict())
                del self.subscriptions_by_id[sub_id]

    def _generate_invoice_events(
        self,
        batch_id: int,
        batch_time: datetime,
        count: int,
        out_list: List[Dict[str, Any]],
        defects: List[str],
    ) -> None:
        for idx in range(count):
            event_time = (batch_time + timedelta(minutes=idx * 2 + 2)).isoformat()
            op_roll = self.rng.random()

            if op_roll < 0.40 or not self.invoices_by_id:
                # INSERT invoice
                self.inv_counter += 1
                inv_id = f"INV-{self.inv_counter:06d}"
                sub_id = (
                    self.rng.choice(list(self.subscriptions_by_id.keys()))
                    if self.subscriptions_by_id
                    else "SUB-000001"
                )
                acc_id = (
                    self.subscriptions_by_id.get(sub_id, {}).get("account_id")
                    if sub_id in self.subscriptions_by_id
                    else "ACC-000001"
                )
                seq = self._next_seq()
                payload = {
                    "invoice_id": inv_id,
                    "account_id": acc_id,
                    "subscription_id": sub_id,
                    "amount": 199.0,
                    "status": "ISSUED",
                    "due_date": (batch_time + timedelta(days=30)).strftime("%Y-%m-%d"),
                }
                self.invoices_by_id[inv_id] = payload
                ev = CDCEvent.create(
                    source_table="invoices",
                    operation=OPERATION_INSERT,
                    business_key=inv_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=payload,
                )
                out_list.append(ev.to_dict())
            else:
                # UPDATE invoice (e.g. mark as PAID)
                inv_id = self.rng.choice(list(self.invoices_by_id.keys()))
                seq = self._next_seq()
                inv = self.invoices_by_id[inv_id]
                inv["status"] = self.rng.choice(INVOICE_STATUSES)
                ev = CDCEvent.create(
                    source_table="invoices",
                    operation=OPERATION_UPDATE,
                    business_key=inv_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=inv,
                )
                out_list.append(ev.to_dict())

    def _generate_payment_events(
        self,
        batch_id: int,
        batch_time: datetime,
        count: int,
        out_list: List[Dict[str, Any]],
        defects: List[str],
    ) -> None:
        for idx in range(count):
            event_time = (batch_time + timedelta(minutes=idx * 2 + 3)).isoformat()
            op_roll = self.rng.random()

            if op_roll < 0.60 or not self.payments_by_id:
                # INSERT payment
                self.pmt_counter += 1
                pmt_id = f"PAY-{self.pmt_counter:06d}"
                inv_id = (
                    self.rng.choice(list(self.invoices_by_id.keys()))
                    if self.invoices_by_id
                    else "INV-000001"
                )
                seq = self._next_seq()
                payload = {
                    "payment_id": pmt_id,
                    "invoice_id": inv_id,
                    "amount": 199.0,
                    "payment_status": "SUCCESS",
                    "processor_ref": f"proc_tx_{self.rng.randint(1000000, 9999999)}",
                }
                self.payments_by_id[pmt_id] = payload
                ev = CDCEvent.create(
                    source_table="payments",
                    operation=OPERATION_INSERT,
                    business_key=pmt_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=payload,
                )
                out_list.append(ev.to_dict())
            else:
                # UPDATE payment status
                pmt_id = self.rng.choice(list(self.payments_by_id.keys()))
                seq = self._next_seq()
                pmt = self.payments_by_id[pmt_id]
                pmt["payment_status"] = self.rng.choice(PAYMENT_STATUSES)
                ev = CDCEvent.create(
                    source_table="payments",
                    operation=OPERATION_UPDATE,
                    business_key=pmt_id,
                    source_sequence=seq,
                    event_timestamp=event_time,
                    ingested_timestamp=batch_time.isoformat(),
                    batch_id=batch_id,
                    payload_dict=pmt,
                )
                out_list.append(ev.to_dict())

    def _inject_batch_defects(
        self,
        batch_id: int,
        batch_time: datetime,
        events_by_table: Dict[str, List[Dict[str, Any]]],
        defects: List[str],
    ) -> None:
        """Inject controlled, deterministic defects into the batch."""
        # 1. Duplicate event injection (duplicate event_id)
        if (
            self.defects.duplicate_event_rate > 0
            and events_by_table["subscriptions"]
            and batch_id % 1 == 0
        ):
            target_event = dict(events_by_table["subscriptions"][0])
            # Append exact copy with same event_id
            events_by_table["subscriptions"].append(target_event)
            defects.append(
                f"Injected DUPLICATE_EVENT: event_id={target_event['event_id']} for subscription {target_event['business_key']}"
            )

        # 2. Out-of-order physical arrival within batch
        if self.defects.out_of_order_rate > 0 and len(events_by_table["accounts"]) >= 3:
            # Swap order of two elements in the list so physical arrival order != source_sequence order
            events_by_table["accounts"][0], events_by_table["accounts"][-1] = (
                events_by_table["accounts"][-1],
                events_by_table["accounts"][0],
            )
            defects.append(
                "Shuffled physical order in accounts.jsonl (demonstrating arrival order != source order)"
            )

        # 3. Hold an event for cross-batch out-of-order arrival
        if (
            self.defects.out_of_order_rate > 0
            and batch_id == 1
            and len(events_by_table["invoices"]) > 2
        ):
            held = events_by_table["invoices"].pop(1)
            self.held_events_for_out_of_order.append(held)
            defects.append(
                f"Held event {held['event_id']} (seq {held['source_sequence']}) to arrive out-of-order in subsequent batch"
            )

        # 4. Late-arriving event injection (source sequence from past range below HWM)
        if self.defects.late_event_rate > 0 and batch_id >= 2:
            # Generate a late event with a sequence lower than current batch minimum sequence
            late_seq = max(1, self.current_sequence - 150)
            late_payload = {
                "subscription_id": "SUB-000005",
                "account_id": "ACC-000001",
                "plan": "BASIC",
                "status": "ACTIVE",
            }
            late_ev = CDCEvent.create(
                source_table="subscriptions",
                operation=OPERATION_UPDATE,
                business_key="SUB-000005",
                source_sequence=late_seq,
                event_timestamp=(batch_time - timedelta(days=2)).isoformat(),
                ingested_timestamp=batch_time.isoformat(),
                batch_id=batch_id,
                payload_dict=late_payload,
            )
            events_by_table["subscriptions"].append(late_ev.to_dict())
            defects.append(
                f"Injected LATE_EVENT: SUB-000005 with seq={late_seq} arriving in batch {batch_id}"
            )

        # 5. Invalid operation injection
        if self.defects.unknown_operation_rate > 0 and batch_id % 2 == 1:
            seq = self._next_seq()
            bad_op_ev = {
                "event_id": generate_event_id("accounts", "ACC-999999", seq, "UPSERT"),
                "source_table": "accounts",
                "operation": "UPSERT",  # Unknown operation!
                "business_key": "ACC-999999",
                "source_sequence": seq,
                "event_timestamp": batch_time.isoformat(),
                "ingested_timestamp": batch_time.isoformat(),
                "batch_id": batch_id,
                "schema_version": CURRENT_SCHEMA_VERSION,
                "payload": json.dumps({"account_id": "ACC-999999", "company_name": "Ghost Corp"}),
            }
            events_by_table["accounts"].append(bad_op_ev)
            defects.append("Injected INVALID_OPERATION: 'UPSERT'")

        # 6. Missing business key injection
        if self.defects.missing_key_rate > 0 and batch_id % 2 == 0:
            seq = self._next_seq()
            missing_key_ev = {
                "event_id": generate_event_id("payments", "", seq, OPERATION_INSERT),
                "source_table": "payments",
                "operation": OPERATION_INSERT,
                "business_key": "",  # Missing business key!
                "source_sequence": seq,
                "event_timestamp": batch_time.isoformat(),
                "ingested_timestamp": batch_time.isoformat(),
                "batch_id": batch_id,
                "schema_version": CURRENT_SCHEMA_VERSION,
                "payload": json.dumps({"amount": 100.0, "payment_status": "SUCCESS"}),
            }
            events_by_table["payments"].append(missing_key_ev)
            defects.append("Injected MISSING_BUSINESS_KEY on payments")

        # 7. Malformed JSON payload injection
        if self.defects.malformed_payload_rate > 0 and batch_id == 2:
            seq = self._next_seq()
            malformed_ev = {
                "event_id": generate_event_id("invoices", "INV-888888", seq, OPERATION_UPDATE),
                "source_table": "invoices",
                "operation": OPERATION_UPDATE,
                "business_key": "INV-888888",
                "source_sequence": seq,
                "event_timestamp": batch_time.isoformat(),
                "ingested_timestamp": batch_time.isoformat(),
                "batch_id": batch_id,
                "schema_version": CURRENT_SCHEMA_VERSION,
                "payload": '{"invoice_id": "INV-888888", "status": "PAID", broken_json...',  # Malformed JSON!
            }
            events_by_table["invoices"].append(malformed_ev)
            defects.append("Injected MALFORMED_PAYLOAD: Unparseable JSON string")


def generate_cdc_batches(
    num_batches: int = 3,
    scale: str = "tiny",
    seed: int = 42,
    output_dir: Optional[Path] = None,
    defects: Optional[DefectConfig] = None,
    schema_transition_batch: Optional[int] = None,
) -> List[Tuple[Dict[str, List[Dict[str, Any]]], BatchManifest]]:
    """Convenience function generating sequential batches starting from batch 1."""
    generator = CDCGenerator(
        scale=scale,
        seed=seed,
        defects=defects,
        schema_transition_batch=schema_transition_batch,
    )
    results = []
    for b_id in range(1, num_batches + 1):
        batch_data, manifest = generator.generate_batch(batch_id=b_id, output_dir=output_dir)
        results.append((batch_data, manifest))
    return results
