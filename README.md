# 🚀 Distributed Message Broker

> A fault-tolerant, replicated message broker built from scratch — inspired by Apache Kafka — using Python, FastAPI, and Docker.

---

## 📖 Overview

This project implements a **distributed message broker system** from first principles. It features a cluster of broker nodes managed by a central coordinator, with automatic leader election, partition-based message routing, active replication, consumer group management, and a real-time monitoring dashboard.

The architecture closely mirrors production-grade systems like Apache Kafka and mirrors how tools such as ZooKeeper coordinate distributed clusters.

---

## ✨ Features

| Feature | Description |
|---|---|
| 🗂️ **Partition-Based Routing** | Messages are routed to specific partitions based on their type (`OTP`, `Alert`, `Info`) |
| 👑 **Leader Election** | The Coordinator automatically elects a leader broker for each partition |
| 🔁 **Active Replication** | Every broker continuously syncs all partitions from their respective leaders, maintaining full replicas |
| 💔 **Fault Tolerance** | If a leader crashes, the Coordinator detects it (heartbeat timeout) and promotes a follower to leader |
| 📦 **Consumer Groups** | Multiple consumers can share the load — each is assigned a unique partition with offset tracking |
| 💾 **Durable Message Log** | Messages are persisted to disk as append-only flat-file logs (one line = one message) |
| 📊 **Live Dashboard** | A real-time web dashboard displays the full cluster state: broker health, partition leaders, and consumer assignments |
| 🤖 **Auto-Producer Bot** | A bot that continuously publishes random messages to stress-test the cluster |
| 🐳 **Fully Dockerized** | One `docker-compose up` command brings up the entire 9-container cluster |

---

## 🏗️ Architecture

```
    ┌──────────────┐   ┌───────────────┐
    │   PRODUCER   │   │ CONSUMER GROUP│
    │(Manual / Bot)│   │ (3 Workers)   │
    └──────────────┘   └───────────────┘
        Produce                Consumer
            │                   │
            ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│                        COORDINATOR                          │
│   (Leader Election · Discovery API · Health Monitoring)     │
│                   http://localhost:8080                     │
└───────────────┬─────────────────┬────────────────┬──────────┘
                │  Heartbeats     │  Discovery     │
    ┌───────────▼──────┐ ┌────────▼──────┐ ┌───────▼────────┐
    │    BROKER - 1    │ │  BROKER - 2   │ │   BROKER - 3   │
    │  (Leader P0, P1) │ │ (Leader P2)   │ │  (Follower)    │
    │  + Replicas P2   │ │ + Replicas    │ │  + Replicas    │
    └──────────────────┘ └───────────────┘ └────────────────┘
```

### Key Components

#### `coordinator.py` — The Cluster Brain
- Receives **heartbeats** from all brokers every 2 seconds
- Tracks broker health and marks brokers as `ALIVE` / `DEAD` (5s timeout)
- Runs a background **leader election loop** every 2 seconds using a round-robin load-balanced assignment
- Exposes a **`/discovery`** endpoint so producers and consumers can find who leads which partition
- Handles **consumer group assignment** via `/assign/{group_id}/{consumer_id}` with load balancing

#### `broker.py` — The Storage & Replication Engine
- Accepts **writes** (produce) only if it is the current leader for that partition
- Serves **reads** (consume) from any broker — followers can serve reads too
- Runs a background **replication sync loop**: every N seconds pulls missing messages from the leader for all non-owned partitions
- Sends a **heartbeat** to the Coordinator every 2 seconds to signal it is alive
- Tracks **consumer offset commits** per group per partition on disk

#### `message_log.py` — The Durable Log
- Simple append-only flat-file log (`log.txt`) per partition
- Messages are 1-indexed by line number (the offset)
- Supports `append_message()` and `read_messages(start_offset)` operations

#### `producer.py` — Interactive CLI Producer
- Routes messages by type using a static key mapping
- Queries `/discovery` to find the correct leader broker
- Supports message types: `OTP`, `Alert`, `Info`

#### `producer_bot.py` — Automated Load Generator
- Continuously sends randomized messages every 3 seconds
- Useful for testing replication, failover, and consumer throughput

#### `consumer_email.py` — Email Notification Consumer
- Joins a consumer group and receives a partition assignment from the Coordinator
- Resumes from the last committed offset (persistent progress tracking)
- Simulates sending emails for each message consumed
- Commits offsets after each successful batch

#### `frontend/index.html` — Live Monitoring Dashboard
- Real-time cluster dashboard served by the Coordinator at `http://localhost:8080`
- Displays broker health, partition-to-leader mapping, and consumer group state
- Auto-refreshes every few seconds

---

## 🚀 Quick Start

### Prerequisites

- [Docker](https://www.docker.com/) & [Docker Compose](https://docs.docker.com/compose/)
- Python 3.10+ (for running individual scripts locally)

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd Project2
```

### 2. Launch the Full Cluster

```bash
docker compose up --build
```

This starts **9 containers**:
- `coordinator` — Cluster brain on port `8080`
- `broker-1`, `broker-2`, `broker-3` — Replicated broker nodes
- `consumer-1`, `consumer-2`, `consumer-3` — Email consumer workers
- `producer-bot` — Automated message producer

> ⏳ The cluster takes ~5–8 seconds to elect leaders and sync before consumers start processing.

### 3. Open the Monitoring Dashboard

Navigate to: **[http://localhost:8080](http://localhost:8080)**

You will see live broker health, partition assignments, and consumer group state.

### 4. Send Messages Manually (Optional)

Run the interactive producer locally:

```bash
# Install dependencies
pip install -r requirements.txt

# Start the interactive producer
python producer.py
```

Then type messages in the format:

```
Enter message to send > Alert: CPU usage is at 99%
Enter message to send > OTP: Your login code is 482910
Enter message to send > Info: New user registered
```

---

## 🧪 Testing Fault Tolerance

### Simulate a Broker Failure

```bash
# Kill broker-1 while the cluster is running
docker stop broker-1
```

Watch the dashboard — within **~5 seconds** the Coordinator will detect the failure, and the partitions formerly owned by `broker-1` will be **automatically reassigned** to a healthy broker (`broker-2` or `broker-3`).

Since all brokers maintain **full replicas** of all partitions, no data is lost.

### Restore the Broker

```bash
docker start broker-1
```

`broker-1` will re-register via heartbeat, resync its replicas, and be re-included in the leader election pool.

---

## 📡 API Reference

### Coordinator API (`http://localhost:8080`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/heartbeat` | Broker registration & heartbeat update |
| `GET` | `/discovery` | Returns partition → leader broker mapping |
| `GET` | `/assign/{group_id}/{consumer_id}` | Assigns a partition to a consumer in a group |
| `GET` | `/state` | Full cluster state (brokers, partitions, consumer groups) |
| `GET` | `/` | Serves the live monitoring dashboard |

### Broker API (`http://broker-N:8000`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/produce/{partition_id}` | Write a message (leader only) |
| `GET` | `/consume/{partition_id}?offset=N` | Read messages from offset N |
| `POST` | `/commit/{group_id}/{partition_id}` | Commit consumer offset |
| `GET` | `/commit/{group_id}/{partition_id}` | Retrieve committed offset |

---

## ⚙️ Configuration

All components are configured via **environment variables**:

| Variable | Default | Component | Description |
|----------|---------|-----------|-------------|
| `COORDINATOR_URL` | `http://coordinator:8080` | Broker, Consumer, Producer | URL of the Coordinator |
| `BROKER_URL` | `http://localhost:8000` | Broker | Broker's own public URL (self-identity) |
| `SYNC_INTERVAL` | `5` | Broker | Seconds between replication sync pulls |
| `GROUP_ID` | `email_alert_group` | Consumer | Consumer group name |
| `CONSUMER_ID` | `email_worker_<pid>` | Consumer | Unique consumer identity |

---

## 📂 Project Structure

```
Project2/
├── coordinator.py        # Cluster coordinator (leader election, discovery, health)
├── broker.py             # Broker node (produce, consume, replication, heartbeat)
├── message_log.py        # Durable append-only file-based message log
├── producer.py           # Interactive CLI message producer
├── producer_bot.py       # Automated random message generator
├── consumer_email.py     # Email notification consumer worker
├── frontend/
│   └── index.html        # Real-time cluster monitoring dashboard
├── Dockerfile            # Docker image for all Python services
├── docker-compose.yml    # Full 9-container cluster definition
├── requirements.txt      # Python dependencies
└── .dockerignore         # Docker build exclusions
```

---

## 🔬 Design Decisions

### Why flat-file logs instead of a database?
Kafka itself uses flat append-only log files. This design ensures high write throughput (sequential I/O is the fastest disk operation), natural offset-based indexing, and easy replication (just sync from offset N onwards).

### Why does the Coordinator wait 5 seconds before assigning leaders?
To avoid the "thundering herd" problem — if leaders were assigned immediately, whichever broker booted first would get all 3 partitions. The 5-second grace period lets the full cluster register before balanced assignment kicks in.

### Why do followers serve reads?
This decouples read scaling from write leadership. Followers can absorb read load while the leader handles writes. Since replication is periodic (every 5s by default), followers may serve slightly stale data — an acceptable tradeoff for throughput.

### Why store consumer offsets on the broker?
This mirrors how Kafka originally stored offsets in ZooKeeper (and later in a special internal topic). Storing offsets on the broker that owns the partition keeps the commit workflow local and simple, while allowing consumers to resume seamlessly after restarts.

---

## 🛠️ Tech Stack

| Technology | Role |
|---|---|
| **Python 3.10** | Core application language |
| **FastAPI** | Async HTTP API framework for Coordinator & Broker |
| **Uvicorn** | ASGI server |
| **httpx** | Async HTTP client for replication & heartbeats |
| **requests** | Sync HTTP client for Producer & Consumer |
| **Docker & Compose** | Containerization & multi-service orchestration |

---

## 📜 License

This project was built as a distributed systems learning exercise. Feel free to use and adapt it.
