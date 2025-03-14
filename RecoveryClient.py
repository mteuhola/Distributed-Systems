import grpc
import recovery_pb2
import recovery_pb2_grpc
import time
import shutil
import os
import hashlib

MONITOR_DIR = "insert/monitored/directory/here"
BACKUP_DIR = os.path.join(MONITOR_DIR, "backup")

def file_hash(file_path):
    """Generate SHA-256 hash of a file for comparison."""
    if not os.path.exists(file_path):
        return None

    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(4096):
            hasher.update(chunk)
    return hasher.hexdigest()

def restore_file(file_name):
    """Restore file from backup."""
    file_name = os.path.basename(file_name)
    backup_path = os.path.join(BACKUP_DIR, file_name)
    original_path = os.path.join(MONITOR_DIR, file_name)
    backup_hash = file_hash(backup_path)
    original_hash = file_hash(original_path)

    if backup_hash == original_hash:
        print(f"⚠️ Skipping recovery: {file_name} is already up-to-date.")
        return True
    elif os.path.exists(backup_path):
        shutil.copy2(backup_path, original_path)
        print(f"✅ Restored file: {original_path}")
        return True
    else:
        print(f"❌ Backup not found: {file_name}")
        return False

def confirm_recovery(file_name):
    """Send confirmation to server after file is restored."""
    try:
        channel = grpc.insecure_channel('localhost:50052')
        stub = recovery_pb2_grpc.RecoveryServiceStub(channel)

        request = recovery_pb2.RecoveryRequest(file_name=file_name)
        response = stub.ConfirmRecovery(request)

        if response.success:
            print(f"✅ Server Response: {response.message}")
        else:
            print(f"❌ Server Response: {response.message}")
            raise Exception(f"Recovery confirmation failed for {file_name}")
    except grpc.RpcError as e:
        print(f"❌ gRPC Error: {e}")
        raise

def process_recovery_queue():
    """Periodically query the server for the next file to recover."""
    try:
        channel = grpc.insecure_channel('localhost:50052')
        stub = recovery_pb2_grpc.RecoveryServiceStub(channel)

        request = recovery_pb2.Empty()
        response_stream = stub.GetFilesToRecover(request)

        for response in response_stream:
            file_name = response.file_name
            if file_name:
                print(f"🔄 Processing recovery for file: {file_name}")

                if restore_file(file_name):
                    confirm_recovery(file_name)
                    print(f"✅ Recovery completed for: {file_name}")
                else:
                    print(f"⚠️ Skipping recovery (backup not found): {file_name}")
            else:
                print("⚠️ No files in the recovery queue.")
        
    except grpc.RpcError as e:
        print(f"❌ gRPC Error during recovery process: {e}")
        time.sleep(5)
    except Exception as ex:
        print(f"❌ Error during recovery: {ex}")
        time.sleep(5)

print("🚀 Recovery Client is running. Waiting for recovery requests...")
while True:
    process_recovery_queue()
    time.sleep(2)
