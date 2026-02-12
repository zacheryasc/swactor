import numpy as np


class Aggregator:
    def __init__(self, num_workers, total_rounds, inbox_addr):
        self.num_workers = num_workers
        self.total_rounds = total_rounds
        self.inbox_addr = inbox_addr
        self.current_round = 0

        self.worker_addrs = []
        self.grad_buffer = []
        self.loss_buffer = []
        self.eval_buffer = []

    def __call__(self, ctx, msg):
        msg_type = msg["type"]

        if msg_type == "init":
            self.worker_addrs = msg["worker_addrs"]
            self.current_round = 0
            self._start_round(ctx)

        elif msg_type == "gradients":
            self.grad_buffer.append(msg["data"])
            self.loss_buffer.append(msg["loss"])

            if len(self.grad_buffer) == self.num_workers:
                # Average gradients across workers
                avg_grads = []
                for layer_grads in zip(*self.grad_buffer):
                    avg = np.mean(layer_grads, axis=0).tolist()
                    avg_grads.append(avg)

                avg_loss = sum(self.loss_buffer) / len(self.loss_buffer)

                # Send averaged gradients to all workers
                for addr in self.worker_addrs:
                    ctx.send(addr, {"type": "update", "data": avg_grads})

                self.current_round += 1

                # Log progress periodically
                if self.current_round % 50 == 0 or self.current_round == self.total_rounds:
                    ctx.send(self.inbox_addr, {
                        "type": "log",
                        "round": self.current_round,
                        "avg_loss": avg_loss,
                    })

                # Clear buffers
                self.grad_buffer = []
                self.loss_buffer = []

                if self.current_round < self.total_rounds:
                    self._start_round(ctx)
                else:
                    # Training done — request evaluation
                    for addr in self.worker_addrs:
                        ctx.send(addr, {"type": "evaluate"})

        elif msg_type == "eval_result":
            self.eval_buffer.append(msg["accuracy"])

            if len(self.eval_buffer) == self.num_workers:
                avg_acc = sum(self.eval_buffer) / len(self.eval_buffer)

                # Save model from worker 0
                ctx.send(self.worker_addrs[0], {
                    "type": "save_model",
                    "path": "mnist_model.pt",
                })

                ctx.send(self.inbox_addr, {
                    "type": "done",
                    "accuracy": avg_acc,
                    "accuracies": list(self.eval_buffer),
                })

    def _start_round(self, ctx):
        for addr in self.worker_addrs:
            ctx.send(addr, {"type": "train_batch"})
