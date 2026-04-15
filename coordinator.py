import time
import asyncio
from typing import Dict, List
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager

# State Management
# Maps broker_url -> last heartbeat timestamp (epoch)
brokers: Dict[str, float] = {}

# Map representing the cluster state (partition_id -> leader_broker_url)
# We initialize it with 3 dummy partitions as an example
partitions: Dict[str, str] = {
    "partition_0": None,
    "partition_1": None,
    "partition_2": None
}

# Consumer group state to remember who was assigned what
# group_id -> { consumer_id -> partition_id }
consumer_groups: Dict[str, Dict[str, str]] = {}

TIMEOUT_SECONDS = 5.0  # Broker is considered dead if no heartbeat for 5 seconds
CLUSTER_START_TIME = time.time()

def reassign_partitions():
    """
    Checks broker health. If a leader goes down, this elects a new leader
    by reassigning the partition to the healthy broker with the least load.
    """
    # Give the cluster 5 seconds to boot up before assigning partitions
    # Otherwise, the very first broker to boot gets assigned ALL partitions
    if time.time() - CLUSTER_START_TIME < 5:
        return
        
    current_time = time.time()
    
    healthy_brokers = [
        b_url for b_url, last_hb in brokers.items()
        if (current_time - last_hb) <= TIMEOUT_SECONDS
    ]
    
    if not healthy_brokers:
        # Cannot assign if cluster is completely down
        return
        
    # Calculate current load so we can map partitions fairly (round-robin style)
    broker_load = {b: 0 for b in healthy_brokers}
    for leader in partitions.values():
        if leader in broker_load:
            broker_load[leader] += 1
            
    for p_id, current_leader in partitions.items():
        if current_leader not in healthy_brokers:
            # The current leader is dead or doesn't exist yet!
            # Pick the healthy broker with the fewest assigned partitions
            new_leader = min(broker_load.keys(), key=lambda k: broker_load[k])
            
            print(f"[Coordinator] Partition {p_id} Leader reassigned: {current_leader} -> {new_leader}")
            partitions[p_id] = new_leader
            broker_load[new_leader] += 1

async def monitor_cluster():
    """Continually monitors broker health in the background."""
    while True:
        reassign_partitions()
        await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting Coordinator Node. Monitoring cluster health...")
    task = asyncio.create_task(monitor_cluster())
    yield
    task.cancel()

app = FastAPI(title="Cluster Coordinator", lifespan=lifespan)

# Mount the frontend static files
app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the monitoring dashboard."""
    return FileResponse("frontend/index.html")

class HeartbeatRequest(BaseModel):
    broker_url: str

@app.post("/heartbeat")
async def receive_heartbeat(payload: HeartbeatRequest):
    """
    1. Every 2 seconds, Brokers send a heartbeat to /heartbeat.
    Registers or updates the last seen time for a broker.
    """
    brokers[payload.broker_url] = time.time()
    reassign_partitions()  # Trigger an immediate check in case this is a new broker joining
    return {"status": "ok"}

@app.get("/discovery")
async def discovery():
    """
    3. Endpoint GET /discovery tells Producers and Consumers which Broker is the Leader for which Partition.
    """
    current_time = time.time()
    healthy_brokers = [
        b_url for b_url, last_hb in brokers.items()
        if (current_time - last_hb) <= TIMEOUT_SECONDS
    ]
    
    return {
        "status": "success",
        "partitions": partitions,
        "active_brokers": healthy_brokers
    }

@app.get("/assign/{group_id}/{consumer_id}")
async def assign_partition_for_consumer(group_id: str, consumer_id: str):
    """
    Dynamically balances and assigns partitions to consumers within a given group.
    """
    if group_id not in consumer_groups:
        consumer_groups[group_id] = {}
        
    group_map = consumer_groups[group_id]
    
    # 1. If consumer already has an assignment, gracefully return it
    if consumer_id in group_map:
        p_id = group_map[consumer_id]
        return {
            "partition_id": p_id,
            "broker_url": partitions.get(p_id)
        }
        
    # 2. Find the partition with the least number of consumers assigned in this group
    assigned_partitions = group_map.values()
    p_load = {p: 0 for p in partitions.keys()}
    for p in assigned_partitions:
        if p in p_load:
            p_load[p] += 1
            
    if not p_load:
        return {"status": "error", "message": "Cluster has zero available partitions."}
        
    chosen_partition = min(p_load.keys(), key=lambda k: p_load[k])
    
    # Track the assignment
    group_map[consumer_id] = chosen_partition
    
    return {
        "partition_id": chosen_partition,
        "broker_url": partitions.get(chosen_partition)
    }

@app.get("/state")
async def get_cluster_state():
    """
    Returns the full, rich cluster state for the monitoring dashboard.
    """
    current_time = time.time()
    
    broker_state = {}
    for b_url, last_hb in brokers.items():
        delta = current_time - last_hb
        broker_state[b_url] = {
            "last_heartbeat_seconds_ago": round(delta, 2),
            "status": "ALIVE" if delta <= TIMEOUT_SECONDS else "DEAD"
        }
    
    return {
        "current_time": current_time,
        "brokers": broker_state,
        "partitions": partitions,
        "consumer_groups": consumer_groups,
    }
