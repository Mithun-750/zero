"""End-to-End Orchestrator for Scenario DL-09: Kafka Partition Leader Failure."""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
for src_dir in [ROOT_DIR / "producer" / "src", ROOT_DIR / "data-prep" / "src"]:
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

from data_prep.load_orders import get_db_connection
from chaos.dl09.generate_events import generate_and_load_manifest
from chaos.dl09.reconcile_results import reconcile_manifest
from producer.config import load_settings
from producer.consumer import OrderConsumer
from producer.database import connect, fetch_pending_events, mark_published, record_publish_failure
from producer.kafka_publisher import KafkaPublisher
from producer.outbox_relay import build_event

logger = logging.getLogger("chaos.dl09.orchestrator")


def get_topic_metadata_via_cli(broker_service: str = "kafka-2") -> dict[int, dict[str, Any]]:
    """Describe orders.created via Kafka CLI on surviving broker and return partition details."""
    cmd = [
        "docker", "compose", "exec", broker_service,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", f"{broker_service}:19092",
        "--describe", "--topic", "orders.created",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    lines = res.stdout.strip().split("\n")

    partitions = {}
    for line in lines:
        if "Partition:" in line:
            parts = line.split("\t")
            part_dict = {}
            for item in parts:
                if ":" in item:
                    k, v = item.strip().split(":", 1)
                    part_dict[k.strip()] = v.strip()
            if "Partition" in part_dict:
                pid = int(part_dict["Partition"])
                partitions[pid] = {
                    "leader": int(part_dict.get("Leader", -1)),
                    "replicas": [int(x) for x in part_dict.get("Replicas", "").split(",") if x],
                    "isr": [int(x) for x in part_dict.get("Isr", "").split(",") if x],
                }
    return partitions


def run_dl09_chaos(
    run_id: str = "DL09-2026-001",
    target_broker_service: str = "kafka-1",
    fail_at_count: int = 500,
    total_events: int = 1000,
) -> dict[str, Any]:
    """Execute DL-09: 1,000-event workload with mid-flight partition leader failure."""
    logger.info("=" * 80)
    logger.info("STARTING SCENARIO DL-09: KAFKA PARTITION LEADER FAILURE")
    logger.info("Test Run ID: %s | Target Broker: %s | Total Events: %s", run_id, target_broker_service, total_events)
    logger.info("=" * 80)

    settings = load_settings()

    # Step 1: Pre-flight cluster inspection
    initial_metadata = get_topic_metadata_via_cli()
    logger.info("Initial Topic Metadata: %s", initial_metadata)
    for p_id, p_info in initial_metadata.items():
        if len(p_info["isr"]) < 3:
            raise RuntimeError(f"Pre-flight check failed: Partition {p_id} ISR={p_info['isr']} < 3")

    target_partition_id = 0
    initial_leader = initial_metadata[target_partition_id]["leader"]
    logger.info(
        "Targeted Partition: %s (Initial Leader: Broker %s, ISR: %s)",
        target_partition_id,
        initial_leader,
        initial_metadata[target_partition_id]["isr"],
    )

    # Step 2: Generate 1,000 test orders & outbox records
    manifest_path = Path("chaos/dl09") / f"manifest_{run_id}.json"
    if not manifest_path.exists():
        manifest_path = generate_and_load_manifest(run_id=run_id, count=total_events)
    else:
        logger.info("Manifest already exists at %s; reusing.", manifest_path)

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    test_event_ids = {e["outbox_event_id"] for e in manifest["events"]}

    # Step 3: Start consumer in background thread with clean consumer group for this run
    consumer_group = f"order-processing-{run_id.lower()}"
    consumer_instance = OrderConsumer(settings, group_id=consumer_group)
    consumer_thread = threading.Thread(
        target=consumer_instance.run,
        kwargs={"max_messages": total_events},
        daemon=True,
        name="ConsumerWorker",
    )
    consumer_thread.start()
    logger.info("Consumer thread launched with consumer group: %s", consumer_group)

    # Give consumer 2 seconds to join group and receive partition assignments
    time.sleep(2)

    # Step 4: Run publisher loop with failure injection
    db_conn = connect(settings)
    publisher = KafkaPublisher(settings)

    published_test_events = 0
    failure_injected = False
    new_leader_detected = False
    new_leader_broker: int | None = None
    new_leader_isr: list[int] = []

    t_start = time.time()
    t_failure: float | None = None
    t_election: float | None = None
    t_producer_recovered: float | None = None

    logger.info("Beginning active publishing of pending outbox events...")

    try:
        while published_test_events < total_events:
            # Poll pending batch
            rows = fetch_pending_events(db_conn, batch_size=settings.outbox_batch_size)
            if not rows:
                time.sleep(0.2)
                continue

            for row in rows:
                row_id_str = str(row["id"])
                if row_id_str not in test_event_ids:
                    continue  # Skip events from other runs

                # Inject failure around fail_at_count
                if not failure_injected and published_test_events >= fail_at_count:
                    logger.info("=" * 80)
                    logger.info(
                        ">>> [CHAOS INJECTION POINT] Published %s/%s events. TERMINATING PARTITION LEADER (%s)...",
                        published_test_events,
                        total_events,
                        target_broker_service,
                    )
                    logger.info("=" * 80)
                    t_failure = time.time()
                    subprocess.run(["docker", "compose", "stop", target_broker_service], check=True)
                    failure_injected = True
                    logger.info(">>> Broker container %s STOPPED at %s", target_broker_service, datetime.now(timezone.utc).isoformat())

                    # Start polling for leader election on partition 0
                    election_check_start = time.time()
                    while time.time() - election_check_start < 30:
                        try:
                            curr_meta = get_topic_metadata_via_cli(broker_service="kafka-2")
                            curr_leader = curr_meta[target_partition_id]["leader"]
                            if curr_leader != initial_leader and curr_leader != -1:
                                t_election = time.time()
                                new_leader_broker = curr_leader
                                new_leader_isr = curr_meta[target_partition_id]["isr"]
                                new_leader_detected = True
                                logger.info("=" * 80)
                                logger.info(
                                    ">>> [LEADER ELECTION DETECTED] Partition %s elected new leader Broker %s (ISR: %s) in %.3fs!",
                                    target_partition_id,
                                    new_leader_broker,
                                    new_leader_isr,
                                    (t_election - t_failure),
                                )
                                logger.info("=" * 80)
                                break
                        except Exception as exc:
                            logger.debug("Leader check retry: %s", exc)
                        time.sleep(0.2)

                try:
                    event = build_event(row)
                    publisher.publish(event, key=str(row["aggregate_id"]))
                    mark_published(db_conn, row["id"])
                    published_test_events += 1

                    if failure_injected and t_producer_recovered is None:
                        t_producer_recovered = time.time()
                        logger.info(
                            ">>> [PRODUCER RECOVERED] Successfully published event_id=%s to new leader in %.3fs after leader failure!",
                            row["id"],
                            (t_producer_recovered - t_failure),
                        )

                    if published_test_events % 100 == 0:
                        logger.info("  Publishing Progress: %s/%s events PUBLISHED", published_test_events, total_events)

                except Exception as exc:
                    record_publish_failure(db_conn, row["id"], exc)
                    logger.warning("Temporary publish failure for event_id=%s (remaining PENDING for retry): %s", row["id"], exc)
                    time.sleep(0.2)

    finally:
        publisher.close()
        db_conn.close()

    logger.info("Outbox publishing complete: %s/%s events published.", published_test_events, total_events)

    # Step 5: Wait for consumer to finish processing all 1,000 events
    logger.info("Waiting for consumer thread to drain all messages...")
    consumer_thread.join(timeout=90)
    consumer_instance.close()

    t_pipeline_drained = time.time()
    logger.info("Consumer drained pipeline at %s", datetime.now(timezone.utc).isoformat())

    # Step 6: Restore the failed broker
    logger.info("Restoring failed broker container (%s)...", target_broker_service)
    t_broker_start = time.time()
    subprocess.run(["docker", "compose", "start", target_broker_service], check=True)
    logger.info("Broker container %s started. Waiting for replica synchronization and full ISR recovery...", target_broker_service)

    isr_recovered = False
    t_isr_restored: float | None = None
    while time.time() - t_broker_start < 60:
        try:
            curr_meta = get_topic_metadata_via_cli(broker_service="kafka-2")
            all_isr_full = all(len(p_info["isr"]) == 3 for p_info in curr_meta.values())
            if all_isr_full:
                t_isr_restored = time.time()
                isr_recovered = True
                logger.info(
                    ">>> [ISR RECOVERY VERIFIED] All 3 replicas rejoined ISR across all partitions in %.3fs!",
                    (t_isr_restored - t_broker_start),
                )
                break
        except Exception as exc:
            logger.debug("ISR check retry: %s", exc)
        time.sleep(1)

    t_complete = time.time()

    # Step 7: Reconcile all 1,000 events
    reconciliation_report = reconcile_manifest(manifest_path)

    # Step 8: RTO and Failure Metrics Calculation
    election_latency = (t_election - t_failure) if (t_election and t_failure) else None
    producer_recovery_latency = (t_producer_recovered - t_failure) if (t_producer_recovered and t_failure) else None
    total_recovery_duration = (t_complete - t_failure) if t_failure else 0

    results = {
        "run_id": run_id,
        "failed_broker": target_broker_service,
        "affected_partition": target_partition_id,
        "initial_leader": initial_leader,
        "new_leader": new_leader_broker,
        "new_leader_isr": new_leader_isr,
        "failure_timestamp": datetime.fromtimestamp(t_failure, tz=timezone.utc).isoformat() if t_failure else None,
        "election_latency_seconds": round(election_latency, 3) if election_latency else None,
        "producer_recovery_latency_seconds": round(producer_recovery_latency, 3) if producer_recovery_latency else None,
        "total_recovery_duration_seconds": round(total_recovery_duration, 3),
        "isr_recovered": isr_recovered,
        "reconciliation": reconciliation_report,
    }

    results_file = Path("chaos/dl09") / f"results_{run_id}.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info("Saved DL-09 results to %s", results_file)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run DL-09 Leader Failure Chaos Experiment.")
    parser.add_argument("--run-id", type=str, default="DL09-2026-001")
    parser.add_argument("--broker", type=str, default="kafka-1")
    parser.add_argument("--fail-at", type=int, default=500)
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_dl09_chaos(
        run_id=args.run_id,
        target_broker_service=args.broker,
        fail_at_count=args.fail_at,
        total_events=args.count,
    )


if __name__ == "__main__":
    main()
