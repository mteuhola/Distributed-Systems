from http import client
import os
import time
import datetime
import shutil
import grpc
import file_monitor_pb2
import file_monitor_pb2_grpc
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import hashlib
from openai import OpenAI

GRPC_SERVER_ADDRESS = "localhost:50051"
MONITOR_DIR = "insert/monitored/directory/here"
BACKUP_DIR = os.path.join(MONITOR_DIR, "backup")
openai_api_key = "openai-api-key-here"

client = OpenAI(
    api_key=openai_api_key,
)


os.makedirs(BACKUP_DIR, exist_ok=True)



def generate_response(user_input):
    """
    Generates a response from OpenAI's GPT model using prompt engineering.
    """
    messages = [
        {"role": "system", "content": "You are an cyber security expert who has special expertise in ransomware detection."},
        {"role": "user", "content": user_input}
    ]
    
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
        temperature=0.7,
        max_tokens=200,
        top_p=1.0,
        frequency_penalty=0.5,
        presence_penalty=0.3
    )

    return response.choices[0].message.content

def hash_file(file_path):
    """Generate SHA-256 hash of a file."""
    try:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(4096):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        print(f"❌ Error hashing file {file_path}: {e}")
        return None
    
def log_message(message):
    print(message)
    date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("file_changes.log", "a") as log_file:
        log_file.write(f'({date_time}) {message}\n')

def send_metadata(file_name, action, flag=True):
    """Send file change metadata via gRPC"""
    try:
        channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
        stub = file_monitor_pb2_grpc.FileMonitorServiceStub(channel)

        timestamp = datetime.datetime.now().isoformat()
        metadata = file_monitor_pb2.FileMetadata(
            file_name=file_name,
            action=action,
            timestamp=timestamp,
            flag=flag
        )

        response = stub.SendFileMetadata(metadata)
        print(f"✅ gRPC Response: success={response.success}")

    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e.code()} - {e.details()}")

def backup_file(file_path):
    """Backup a file to the backup directory"""
    if os.path.isfile(file_path):  # Ensure it's a file
        file_name = os.path.basename(file_path)
        backup_path = os.path.join(BACKUP_DIR, file_name)
        try:
            shutil.copy2(file_path, backup_path)
            print(f"📂 Backup created: {backup_path}")
        except Exception as e:
            print(f"❌ Backup failed for {file_path}: {e}")

def backup_existing_files():
    """Backup all existing files in the monitored directory"""
    for file_name in os.listdir(MONITOR_DIR):
        file_path = os.path.join(MONITOR_DIR, file_name)
        if os.path.isfile(file_path):
            backup_file(file_path)

def read_file_content(file_path):
    """Reads the content of a file, safely handling encoding issues."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception as e:
        print(f"❌ Error reading file {file_path}: {e}")
        return None

class FileChangeHandler(FileSystemEventHandler):
    def process_event(self, event, event_type):
        """Process file events: log, send gRPC, and analyze content with AI"""
        if not event.is_directory:
            file_path = os.path.abspath(event.src_path)
            file_hash = hash_file(file_path)
            file_content = read_file_content(file_path)

            log_message(f"{event_type} file: {file_path} | Hash: {file_hash}")

            flag = True  

            if file_content:
                def ai_analysis():
                    nonlocal flag
                    ai_response = generate_response(
                        f"A file has changed: {file_path}.\n"
                        f"SHA-256 Hash: {file_hash}.\n"
                        f"Content Preview:\n{file_content[:1000]}..."
                        f"\nIs this change suspicious? Reply with 'malicious' or 'safe' and explain why."
                    )
                    print(f"🤖 AI Response: {ai_response}")

                    if "safe" in ai_response.lower():
                        flag = False

                    send_metadata(file_path, event_type, flag)

                ai_thread = threading.Thread(target=ai_analysis)
                ai_thread.start()
                ai_thread.join(timeout=10)

                if ai_thread.is_alive():
                    print("⏳ AI response timed out. Marking as suspicious.")
                    send_metadata(file_path, event_type, flag=True)

            if event_type == "created" and not flag:
                backup_file(file_path)

    def on_modified(self, event):
        self.process_event(event, "modified")

    def on_created(self, event):
        self.process_event(event, "created")

    def on_deleted(self, event):
        self.process_event(event, "deleted")

    def on_moved(self, event):
        """Handle file renames/moves"""
        if not event.is_directory:
            old_path = os.path.abspath(event.src_path)
            new_path = os.path.abspath(event.dest_path)
            log_message(f"Renamed file: {old_path} -> {new_path}")
            send_metadata(f"{old_path} -> {new_path}", "renamed", flag=True)

def monitor_directory():
    """Start monitoring directory for changes"""
    observer = Observer()
    event_handler = FileChangeHandler()
    observer.schedule(event_handler, MONITOR_DIR, recursive=True)
    observer.start()

    print("👀 Monitoring directory for changes...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    backup_existing_files()
    monitor_directory()
