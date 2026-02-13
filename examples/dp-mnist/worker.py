import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Subset
from torchvision import datasets, transforms


class MnistNet(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc1 = nn.Linear(784, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = x.view(-1, 784)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


class MnistWorker:
    def __init__(self, agg_addr, worker_id, initial_weights, batch_size, lr):
        self.agg_addr = agg_addr
        self.worker_id = worker_id
        self.batch_size = batch_size

        self.model = MnistNet()
        # Apply initial weights so all workers start from the same point
        for param, w in zip(self.model.parameters(), initial_weights):
            param.data = torch.tensor(w, dtype=param.dtype).reshape(param.shape)

        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307,), (0.3081,)),
        ])
        full_train = datasets.MNIST("./data", train=True, download=True, transform=transform)

        # Shard: worker 0 gets first half, worker 1 gets second half
        n = len(full_train)
        half = n // 2
        if worker_id == 0:
            indices = list(range(0, half))
        else:
            indices = list(range(half, n))
        shard = Subset(full_train, indices)
        self.train_loader = DataLoader(shard, batch_size=batch_size, shuffle=True)
        self.train_iter = iter(self.train_loader)

        self.test_loader = DataLoader(
            datasets.MNIST("./data", train=False, download=True, transform=transform),
            batch_size=1000,
            shuffle=False,
        )

    def _next_batch(self):
        try:
            return next(self.train_iter)
        except StopIteration:
            self.train_iter = iter(self.train_loader)
            return next(self.train_iter)

    def __call__(self, ctx, msg):
        msg_type = msg["type"]

        if msg_type == "train_batch":
            self.optimizer.zero_grad()
            data, target = self._next_batch()
            output = self.model(data)
            loss = F.nll_loss(output, target)
            loss.backward()

            # Extract gradients as flat lists
            grads = [p.grad.detach().flatten().tolist() for p in self.model.parameters()]
            ctx.send(self.agg_addr, {
                "type": "gradients",
                "worker_id": self.worker_id,
                "data": grads,
                "loss": loss.item(),
            })

        elif msg_type == "update":
            avg_grads = msg["data"]
            for param, g in zip(self.model.parameters(), avg_grads):
                param.grad = torch.tensor(g, dtype=param.dtype).reshape(param.shape)
            self.optimizer.step()

        elif msg_type == "evaluate":
            self.model.eval()
            correct = 0
            total = 0
            with torch.no_grad():
                for data, target in self.test_loader:
                    output = self.model(data)
                    pred = output.argmax(dim=1)
                    correct += pred.eq(target).sum().item()
                    total += len(target)
            self.model.train()
            ctx.send(self.agg_addr, {
                "type": "eval_result",
                "worker_id": self.worker_id,
                "accuracy": correct / total,
            })

        elif msg_type == "save_model":
            path = msg.get("path", "mnist_model.pt")
            torch.save(self.model.state_dict(), path)
