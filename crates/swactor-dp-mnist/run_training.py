#!/usr/bin/env python3
"""Data-parallel MNIST training using swactor actors.

Spawns an Aggregator and two MnistWorker actors.  Workers each train on
half the dataset; the Aggregator averages their gradients every round.
"""

import sys
import time

import torch
from swactor import Runtime, RuntimeConfig

from aggregator import Aggregator
from worker import MnistNet, MnistWorker

NUM_WORKERS = 2
TOTAL_ROUNDS = 750
BATCH_SIZE = 64
LEARNING_RATE = 0.01


def extract_weights(model):
    """Return model parameters as a list of flat Python lists."""
    return [p.detach().flatten().tolist() for p in model.parameters()]


def main():
    # 1. Create multi-threaded runtime (2 worker threads)
    rt = Runtime(RuntimeConfig(num_threads=NUM_WORKERS))

    # 2. Create main inbox for log/done messages
    inbox = rt.inbox()

    # 3. Create reference model and extract initial weights
    ref_model = MnistNet()
    initial_weights = extract_weights(ref_model)
    del ref_model

    # 4. Spawn Aggregator
    agg_handler = Aggregator(
        num_workers=NUM_WORKERS,
        total_rounds=TOTAL_ROUNDS,
        inbox_addr=inbox.addr,
    )
    agg_addr = rt.spawn(agg_handler)

    # 5. Spawn workers (same initial weights, different data shards)
    worker_addrs = []
    for wid in range(NUM_WORKERS):
        worker = MnistWorker(
            agg_addr=agg_addr,
            worker_id=wid,
            initial_weights=initial_weights,
            batch_size=BATCH_SIZE,
            lr=LEARNING_RATE,
        )
        addr = rt.spawn(worker)
        worker_addrs.append(addr)

    # 6. Send "init" to aggregator
    rt.send(agg_addr, {
        "type": "init",
        "worker_addrs": worker_addrs,
    })

    # 7. Start background worker threads
    handle = rt.run()

    t0 = time.time()

    print(f"Starting data-parallel MNIST training: {TOTAL_ROUNDS} rounds, "
          f"{NUM_WORKERS} workers, batch_size={BATCH_SIZE}, lr={LEARNING_RATE}")
    print("-" * 60)

    # 8. Poll inbox until "done"
    done = False
    while not done:
        msg = inbox.try_recv()
        if msg is None:
            time.sleep(0.01)
            continue

        if msg["type"] == "log":
            elapsed = time.time() - t0
            print(f"  Round {msg['round']:>4d}/{TOTAL_ROUNDS}  "
                  f"avg_loss={msg['avg_loss']:.4f}  "
                  f"elapsed={elapsed:.1f}s")

        elif msg["type"] == "done":
            elapsed = time.time() - t0
            print("-" * 60)
            print(f"Training complete in {elapsed:.1f}s")
            for i, acc in enumerate(msg["accuracies"]):
                print(f"  Worker {i} accuracy: {acc:.2%}")
            print(f"  Average accuracy:  {msg['accuracy']:.2%}")
            print(f"Model saved to mnist_model.pt")
            done = True

    # 9. Shutdown worker threads
    handle.shutdown()
    handle.join()

    return 0


if __name__ == "__main__":
    sys.exit(main())
