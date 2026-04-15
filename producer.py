import os
import requests

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8080")

# 1. Routing Table mapping Types to specific Partitions
KEY_MAPPING = {
    "otp": "partition_0",
    "alert": "partition_1",
    "info": "partition_2"
}

def publish_message(msg_type: str, message: str):
    msg_type_clean = msg_type.lower().strip()
    
    if msg_type_clean not in KEY_MAPPING:
        print(f"[Error] Unknown message type '{msg_type}'. Expected one of: OTP, Alert, Info")
        return
        
    target_partition = KEY_MAPPING[msg_type_clean]
    print(f"Analyzing route -> Type '{msg_type_clean}' targets '{target_partition}'")
    
    # 2. Ask the Coordinator for the Leader's IP for that partition
    try:
        discovery_resp = requests.get(f"{COORDINATOR_URL}/discovery")
        discovery_resp.raise_for_status()
        data = discovery_resp.json()
        
        # Find which Broker currently owns our target partition
        leader_broker_url = data.get("partitions", {}).get(target_partition)
        
        if not leader_broker_url:
            print(f"[Error] Coordinator claims {target_partition} currently has NO active Leader broker!")
            return
            
    except requests.exceptions.RequestException as e:
        print(f"[Error] Could not reach Coordinator to discover topology: {e}")
        return

    # 3. Send the message to that Broker using a POST request
    try:
        # We package the message data via JSON block
        payload = {"message": f"[{msg_type_clean.upper()}] {message}"}
        
        produce_resp = requests.post(
            f"{leader_broker_url}/produce/{target_partition}",
            json=payload
        )
        produce_resp.raise_for_status()
        
        result = produce_resp.json()
        offset = result.get("offset", "?")
        print(f"[Success] Message saved on {leader_broker_url} ({target_partition}) at internal offset {offset}")
        
    except requests.exceptions.RequestException as e:
        print(f"[Error] Failed to transmit message to Broker leader at {leader_broker_url}: {e}")

if __name__ == "__main__":
    print("========== Distributed Message Producer ==========")
    print("Valid Keys: OTP, Alert, Info")
    print("Example Usage: 'Alert: The CPU is overheating!'")
    print("Type 'exit' to quit.")
    print("==================================================")
    
    # Interactive loop to easily test manual messaging
    while True:
        try:
            user_input = input("\nEnter message to send > ")
            if user_input.strip().lower() in ('exit', 'quit'):
                break
                
            if ":" not in user_input:
                print("Format error! Please use the 'Type: Message' format. (e.g. Info: New user signed up)")
                continue
                
            # Split the input
            parsed_type, parsed_content = user_input.split(":", 1)
            publish_message(parsed_type, parsed_content.strip())
            
        except KeyboardInterrupt:
            print("\nShutting down Producer...")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
