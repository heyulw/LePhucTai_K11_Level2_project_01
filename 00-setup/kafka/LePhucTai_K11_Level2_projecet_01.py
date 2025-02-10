import configparser
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from pymongo import MongoClient

# Đọc file cấu hình
config = configparser.ConfigParser()
config.read('mykafka.conf')

# Chuyển đổi các section thành dictionary
source_kafka_config = dict(config.items('source_kafka'))
destination_kafka_config = dict(config.items('destination_kafka'))

# Kết nối đến MongoDB
mongo_client = MongoClient("mongodb+srv://<db_username>:<db_password>@cluster0.wwj0k.mongodb.net/")
database = mongo_client["kafka_data"]
collection = database["processed_data"]

source_topic = 'product_view'
destination_topic = 'processed_data'

# Tạo Kafka Consumer và Producer từ cấu hình đã đọc
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
            else:
                raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            print(f"Producing message to {destination_topic}: {message_value}")

            destination_producer.produce(destination_topic, msg.value())
            destination_producer.flush()

            document = {
                "topic": source_topic,
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
