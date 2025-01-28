from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from pymongo import MongoClient


source_kafka_config = {
    'bootstrap.servers': '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'source-group',
    'auto.offset.reset': 'earliest',
}


destination_kafka_config = {
    'bootstrap.servers': 'localhost:9094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024',
}
mongo_client = MongoClient("mongodb+srv://tailephuc:tailephuc@cluster0.wwj0k.mongodb.net/")
database = mongo_client["kafka_data"]
collection = database["message"]
source_topic = 'product_view'
destination_topic = 'test'
source_consumer = Consumer(source_kafka_config)
destination_producer = Producer(destination_kafka_config)
source_consumer.subscribe([source_topic])
print(f"Reading from source topic '{source_topic}' and producing to '{destination_topic}'...")
try:
    while True:
        msg = source_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()} at offset {msg.offset()}")
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            print(f"Producing message to {destination_topic}: {message_value}")
            destination_producer.produce(destination_topic, msg.value())
            destination_producer.flush()
            document = {
                "topic": destination_topic,
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode('utf-8') if msg.key() else None,
                "value": message_value
            }
            collection.insert_one(document)
            print(f"Saved to MongoDB: {document}")

except KeyboardInterrupt:
    print("Aborted by user")

finally:
    source_consumer.close()
    mongo_client.close()
