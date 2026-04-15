import os
from typing import List

class MessageLog:
    def __init__(self, partition_dir: str):
        """
        Initializes the MessageLog for a specific partition.
        Creates the folder and log file if they don't exist.
        """
        self.partition_dir = partition_dir
        self.log_file_path = os.path.join(self.partition_dir, 'log.txt')
        
        # Create directory if it doesn't exist
        os.makedirs(self.partition_dir, exist_ok=True)
        
        # Create file if it doesn't exist
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, 'w') as f:
                pass
                
        # Calculate current offset based on existing lines (0-indexed)
        self.current_offset = self._get_current_offset()

    def _get_current_offset(self) -> int:
        offset = 0
        with open(self.log_file_path, 'r') as f:
            for _ in f:
                offset += 1
        return offset

    def append_message(self, data: str) -> int:
        """
        Appends a string message to the log file and returns its offset.
        """
        # Ensure data does not contain newlines to keep 1 message per line
        safe_data = data.replace('\n', ' ')
        
        with open(self.log_file_path, 'a') as f:
            f.write(f"{safe_data}\n")
            
        assigned_offset = self.current_offset
        self.current_offset += 1
        
        return assigned_offset

    def read_messages(self, start_offset: int) -> List[str]:
        """
        Returns all messages from the log file starting from the given offset.
        """
        messages = []
        with open(self.log_file_path, 'r') as f:
            for line_number, line in enumerate(f):
                if line_number >= start_offset:
                    # Remove the trailing newline
                    messages.append(line.rstrip('\n'))
        return messages
