import grpc
from kafka import KafkaConsumer
import json
import recovery_pb2
import recovery_pb2_grpc

def send_add_to_recovery_queue_request(file_name):
    """Send a request to the server to add the file to the recovery queue."""
    try:
        channel = grpc.insecure_channel('storage-manager:50052')  # Adjust the gRPC server address
        stub = recovery_pb2_grpc.RecoveryServiceStub(channel)

        request = recovery_pb2.RecoveryRequest(file_name=file_name)
        print(f"🔄 Sending AddToRecoveryQueue request for file: {file_name}")  # Debugging line
        response = stub.AddToRecoveryQueue(request)

        print(f"🔄 Added to Recovery Queue: success={response.success}, message={response.message}")
    except Exception as e:
        print(f"❌ gRPC Request Failed: {e}")

def start_kafka_consumer():
    """Start the Kafka Consumer and process incoming messages."""
    try:
        consumer = KafkaConsumer(
            'alert',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='recovery_group',
            auto_offset_reset='latest',
            enable_auto_commit=False
        )
        print("✅ Kafka Consumer started, waiting for messages...")

        latest_message = None

        for message in consumer:
            try:
                data = message.value
                file_name = data.get("file_name", "Unknown")

                print(f"📥 New Kafka message received: {data}")


                if latest_message != file_name:
                    latest_message = file_name
                    send_add_to_recovery_queue_request(file_name)
                    consumer.commit()
                else:
                    print("⚠️ Duplicate message received, skipping...")

            except Exception as e:
                print(f"❌ Error processing message: {e}")
    except Exception as e:
        print(f"❌ Kafka Consumer Error: {e}")

if __name__ == "__main__":
    start_kafka_consumer()
