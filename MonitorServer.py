import grpc
from concurrent import futures
import file_monitor_pb2
import file_monitor_pb2_grpc
from pymongo import MongoClient
from kafka import KafkaProducer
import json
from bson import ObjectId

class FileMonitorService(file_monitor_pb2_grpc.FileMonitorServiceServicer):
    def __init__(self):
        print("🚀 Starting gRPC Server...")

        try:
            self.mongo_client = MongoClient("mongodb://root:rootpassword@mongodb:27017/file_monitor_db?authSource=admin")
            self.db = self.mongo_client['file_monitor_db']
            self.collection = self.db['file_metadata']
            print("✅ Connected to MongoDB")
        except Exception as e:
            print(f"❌ MongoDB connection failed: {e}")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connected to Kafka")
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")

    def SendFileMetadata(self, request, context):
        file_metadata = {
            "file_name": request.file_name,
            "action": request.action,
            "timestamp": request.timestamp,
            "flag": request.flag
        }

        try:
            result = self.collection.insert_one(file_metadata)
            print(f"📂 Saved to MongoDB: {file_metadata}")
        except Exception as e:
            print(f"❌ MongoDB Insert Failed: {e}")

        if request.flag:
            kafka_message = {
                "file_id": str(result.inserted_id),
                "file_name": request.file_name,
                "action": request.action,
                "timestamp": request.timestamp
        }
            try:
                self.producer.send('alert', value=kafka_message)
                print(f"📡 Sent to Kafka: {kafka_message}")
            except Exception as e:
                print(f"❌ Kafka Send Failed: {e}")

        return file_monitor_pb2.Ack(success=True)

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
file_monitor_pb2_grpc.add_FileMonitorServiceServicer_to_server(FileMonitorService(), server)
server.add_insecure_port('[::]:50051')

print("✅ gRPC Server is running on port 50051")
server.start()
server.wait_for_termination()
