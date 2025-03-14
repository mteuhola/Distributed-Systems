from concurrent import futures
import grpc
import queue
import recovery_pb2
import recovery_pb2_grpc


recovery_queue = queue.Queue()

class RecoveryService(recovery_pb2_grpc.RecoveryServiceServicer):

    def AddToRecoveryQueue(self, request, context):
        """Add file to recovery queue."""
        file_name = request.file_name
        recovery_queue.put(file_name)
        return recovery_pb2.RecoveryResponse(success=True, message=f"File {file_name} added to recovery queue.")

    def GetFilesToRecover(self, request, context):
        """Query the server for files to recover."""
        while not recovery_queue.empty():
            file_name = recovery_queue.get()
            print(f"🔄 Returning file '{file_name}' for recovery.")
            yield recovery_pb2.FileName(file_name=file_name)

    def ConfirmRecovery(self, request, context):
        """Client calls this to confirm file recovery."""
        file_name = request.file_name
        print(f"✅ Confirmation received for file: {file_name}")

        if "error" in file_name:
            print(f"❌ Recovery failed for file: {file_name}")
            return recovery_pb2.RecoveryResponse(success=False, message=f"Recovery failed for {file_name}")

        print(f"✅ Recovery successful for file: {file_name}")
        return recovery_pb2.RecoveryResponse(success=True, message="Confirmation received.")

def serve():
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    recovery_pb2_grpc.add_RecoveryServiceServicer_to_server(RecoveryService(), server)
    server.add_insecure_port('[::]:50052')
    print("🚀 Recovery gRPC Server is running on port 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
