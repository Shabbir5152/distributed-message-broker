import os
import time
import requests

# Execution Config
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8080")
GROUP_ID = os.getenv("GROUP_ID", "email_alert_group")
CONSUMER_ID = os.getenv("CONSUMER_ID", f"email_worker_{os.getpid()}")

def start_polling():
    print(f"Starting Email Consumer Worker [{CONSUMER_ID}] in group [{GROUP_ID}]")

    # 1. Ask Coordinator: "Which partition should I handle?"
    partition_id = None
    broker_url = None
    
    print(f"Contacting Coordinator at {COORDINATOR_URL} for partition assignment...")
    while not partition_id or not broker_url:
        try:
            assignment_response = requests.get(f"{COORDINATOR_URL}/assign/{GROUP_ID}/{CONSUMER_ID}")
            assignment_response.raise_for_status()
            
            assignment = assignment_response.json()
            partition_id = assignment.get("partition_id")
            broker_url = assignment.get("broker_url")
            
            if not partition_id or not broker_url:
                print("Coordinator has not elected a Leader for this partition yet. Waiting for Brokers to sync... (Retrying in 3s)")
                time.sleep(3)
                continue
                
            print(f"Success! Assigned to {partition_id} managed by Leader {broker_url}")
        except Exception as e:
            print(f"Failed to fetch assignment. Coordinator might still be booting. (Retrying in 3s)")
            time.sleep(3)

    # 2. Fetch messages starting from the last committed offset
    try:
        commit_resp = requests.get(f"{broker_url}/commit/{GROUP_ID}/{partition_id}")
        commit_resp.raise_for_status()
        current_offset = commit_resp.json().get("committed_offset", 0)
        print(f"[Partition: {partition_id}] Resuming from Offset: {current_offset}")
    except Exception as e:
        print(f"Could not connect to broker {broker_url} to get offset: {e}")
        return

    print("\nStarting Email Processing Loop...")
    while True:
        try:
            # Poll Broker for new messages
            consume_resp = requests.get(f"{broker_url}/consume/{partition_id}", params={"offset": current_offset})
            consume_resp.raise_for_status()
            
            data = consume_resp.json()
            messages = data.get("messages", [])
            
            if not messages:
                # Idle state, wait a bit before polling again
                time.sleep(2)
                continue
                
            # 3. For each message, pretend to send an email
            for msg in messages:
                # The user expected printed formatting: 'Sending Email to [User]'
                print(f"*** Sending Email to [{msg}] ***")
                
            # Update our tracker to expect next batch
            current_offset += len(messages)
            
            # 4. After processing the batch, call commit endpoint to save progress
            commit_payload = {"offset": current_offset}
            requests.post(f"{broker_url}/commit/{GROUP_ID}/{partition_id}", json=commit_payload)
            
            print(f"[Partition: {partition_id}] Batch flushed successfully. Committed new offset: {current_offset}")
            
        except requests.exceptions.RequestException as e:
            print(f"[Error] Lost connection to broker: {e}")
            print("Waiting 5 seconds before retrying...")
            time.sleep(5)
            
        time.sleep(1) # Prevent aggressively overwhelming the broker

if __name__ == "__main__":
    start_polling()
