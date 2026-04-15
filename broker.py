import os
import asyncio
import httpx
from typing import Dict, List
from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
from message_log import MessageLog

# Configuration (Driven by Environment Variables)
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "5")) # Delay between sync pulls
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8080") # Zookeeper/Coordinator URL
BROKER_URL = os.getenv("BROKER_URL", "http://localhost:8000") # My own identity URL

# Dynamic Role Management
leadership_status = set()  # Partitions this broker currently leads

# Cache to hold instantiated MessageLogs for each partition
partitions: Dict[str, MessageLog] = {}

def get_partition(partition_id: str) -> MessageLog:
    """Helper to retrieve or initialize a MessageLog for a given partition."""
    if partition_id not in partitions:
        # We store all partitions inside a 'data' folder for cleanliness
        partition_dir = os.path.join("data", partition_id)
        partitions[partition_id] = MessageLog(partition_dir)
    return partitions[partition_id]

async def sync_with_leader():
    """
    Background loop that continuously polls the Coordinator for the topology matrix.
    If it is a Follower for a partition, it pulls and replicates data from the Leader.
    """
    global leadership_status
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # 1. Ask Coordinator for the latest partition matrix
                discovery_resp = await client.get(f"{COORDINATOR_URL}/discovery")
                if discovery_resp.status_code == 200:
                    topology = discovery_resp.json().get("partitions", {})
                    
                    new_leadership = set()
                    
                    for partition_id, leader_url in topology.items():
                        if not leader_url:
                            continue
                            
                        if leader_url == BROKER_URL:
                            # We are the leader! No need to sync.
                            new_leadership.add(partition_id)
                        else:
                            # We are a follower. Replicate missing data.
                            try:
                                log = get_partition(partition_id)
                                current_offset = log.current_offset
                                
                                sync_resp = await client.get(
                                    f"{leader_url}/consume/{partition_id}",
                                    params={"offset": current_offset}
                                )
                                
                                if sync_resp.status_code == 200:
                                    messages = sync_resp.json().get("messages", [])
                                    for msg in messages:
                                        log.append_message(msg)
                                    if messages:
                                        print(f"[Replica] Synced {len(messages)} items for {partition_id} from {leader_url}")
                            except Exception as e:
                                print(f"[Replica Error] Syncing {partition_id} from {leader_url} failed: {e}")
                                
                    leadership_status = new_leadership
            except Exception as e:
                pass # Coordinator might be unreachable temporarily
                
            await asyncio.sleep(SYNC_INTERVAL)

async def send_heartbeats():
    """
    Background loop that sends a heartbeat to the Coordinator every 2 seconds.
    """
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.post(
                    f"{COORDINATOR_URL}/heartbeat",
                    json={"broker_url": BROKER_URL}
                )
            except Exception as e:
                # If coordinator is unreachable, silently fail and retry later
                # print(f"Heartbeat failed: {e}") - Avoid spamming the logs
                pass
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    print(f"Starting Broker at {BROKER_URL}. Launching dynamic Replication Sync engine...")
    sync_task = asyncio.create_task(sync_with_leader())
        
    print(f"Starting heartbeat to Coordinator at {COORDINATOR_URL} as {BROKER_URL}")
    heartbeat_task = asyncio.create_task(send_heartbeats())
        
    yield
    
    # SHUTDOWN
    if sync_task:
        sync_task.cancel()
    heartbeat_task.cancel()

app = FastAPI(title="Distributed Broker API", lifespan=lifespan)

# Request Models
class ProduceRequest(BaseModel):
    message: str

class CommitRequest(BaseModel):
    offset: int

@app.post("/produce/{partition_id}")
async def produce_message(partition_id: str, payload: ProduceRequest):
    """
    1. POST /produce/{partition_id}
    Takes a message and calls append_message. Reject writes if not the leader.
    """
    if partition_id not in leadership_status:
        return {
            "status": "error", 
            "message": f"Write Rejected. This broker is currently a FOLLOWER for {partition_id}."
        }
        
    log = get_partition(partition_id)
    offset = log.append_message(payload.message)
    return {
        "status": "success", 
        "partition_id": partition_id, 
        "offset": offset
    }

@app.get("/consume/{partition_id}")
async def consume_messages(partition_id: str, offset: int = 0):
    """
    2. GET /consume/{partition_id}
    Takes an offset and returns messages. (Both Leader and Follower can serve reads)
    """
    log = get_partition(partition_id)
    messages = log.read_messages(offset)
    return {
        "status": "success", 
        "partition_id": partition_id, 
        "next_offset": offset + len(messages),
        "messages": messages
    }

@app.post("/commit/{group_id}/{partition_id}")
async def commit_offset(group_id: str, partition_id: str, payload: CommitRequest):
    """
    3. POST /commit/{group_id}/{partition_id}
    Stores the current offset for a consumer group in a special file on the broker.
    """
    # Create the special folder for consumer offsets
    base_dir = os.path.join("data", "consumer_offsets", group_id)
    os.makedirs(base_dir, exist_ok=True)
    
    # Write the offset to a partition-specific file
    offset_file = os.path.join(base_dir, f"{partition_id}_offset.txt")
    with open(offset_file, "w") as f:
        f.write(str(payload.offset))
        
    return {
        "status": "success", 
        "group_id": group_id, 
        "partition_id": partition_id, 
        "committed_offset": payload.offset
    }

@app.get("/commit/{group_id}/{partition_id}")
async def get_commit_offset(group_id: str, partition_id: str):
    """Utility endpoint to retrieve the committed offset."""
    base_dir = os.path.join("data", "consumer_offsets", group_id)
    offset_file = os.path.join(base_dir, f"{partition_id}_offset.txt")
    
    if not os.path.exists(offset_file):
        return {"status": "success", "group_id": group_id, "partition_id": partition_id, "committed_offset": 0}
        
    with open(offset_file, "r") as f:
        try:
            offset = int(f.read().strip())
        except ValueError:
            offset = 0
            
    return {"status": "success", "group_id": group_id, "partition_id": partition_id, "committed_offset": offset}
