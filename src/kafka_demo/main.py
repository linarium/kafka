from __future__ import annotations

import logging
import os
import re
import signal
import threading
import time

from kafka_demo import producer_worker
from kafka_demo.consumers import run_batch_message_consumer, run_single_message_consumer

logger = logging.getLogger(__name__)


def _should_run_producer() -> bool:
    v = os.getenv("ENABLE_PRODUCER", "").strip().lower()
    if v in ("0", "false", "no"):
        return False
    if v in ("1", "true", "yes"):
        return True
    h = os.getenv("HOSTNAME", "")
    m = re.search(r"[_-](\d+)$", h)
    if m:
        return int(m.group(1)) == 1
    return True


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s %(message)s",
    )

    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("TOPIC", "homework-events")

    stop_event = threading.Event()

    def _handle_sig(*_: object) -> None:
        logger.info("Сигнал остановки")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    threads: list[threading.Thread] = []

    if _should_run_producer():
        t_prod = threading.Thread(
            target=producer_worker.run_producer_loop,
            name="producer",
            args=(bootstrap, topic, stop_event),
            daemon=True,
        )
        threads.append(t_prod)
    else:
        logger.info("Продюсер отключён")

    t_single = threading.Thread(
        target=run_single_message_consumer,
        name="single-consumer",
        args=(bootstrap, topic, stop_event),
        daemon=True,
    )
    threads.append(t_single)

    t_batch = threading.Thread(
        target=run_batch_message_consumer,
        name="batch-consumer",
        args=(bootstrap, topic, stop_event),
        daemon=True,
    )
    threads.append(t_batch)

    for t in threads:
        t.start()

    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads:
        t.join(timeout=15)


if __name__ == "__main__":
    main()
