import os
import time
import requests
import random

# When running inside Docker, this will hit the internal container name "coordinator"
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8080")

KEY_MAPPING = {
    "otp": "partition_0",
    "alert": "partition_1",
    "info": "partition_2"
}

def start_bot():
    print("🤖 Starting Automated Producer Bot...")
    
    # Wait for cluster to initialize and elect leaders (takes ~5 seconds)
    print("🤖 Waiting 8 seconds for the cluster to elect leaders before blasting messages...")
    time.sleep(8)
    
    counter = 1
    while True:
        msg_type = random.choice(list(KEY_MAPPING.keys()))
        target_partition = KEY_MAPPING[msg_type]
        payload = f"Automated Random Data #{counter} for {msg_type.upper()}"
        
        try:
            # 1. Ask coordinator who the leader is
            discovery_resp = requests.get(f"{COORDINATOR_URL}/discovery")
            discovery_resp.raise_for_status()
            data = discovery_resp.json()
            
            leader_broker_url = data.get("partitions", {}).get(target_partition)
            
            if leader_broker_url:
                # 2. Transmit message
                produce_resp = requests.post(
                    f"{leader_broker_url}/produce/{target_partition}",
                    json={"message": payload}
                )
                produce_resp.raise_for_status()
                offset = produce_resp.json().get("offset", "?")
                print(f"\n🚀 [PRODUCED] '{msg_type.upper()}' -> {target_partition} on {leader_broker_url} (Offset: {offset})")
            else:
                print(f"🤖 [Auto-Producer] Warning: No leader found for {target_partition}")
                
        except Exception as e:
            print(f"🤖 [Auto-Producer] Error communicating with cluster: {e}")
            
        counter += 1
        time.sleep(3)  # Send a random message every 3 seconds

if __name__ == "__main__":
    start_bot()
