from google.cloud import pubsub_v1
import json
import uuid
import time

topic_id = "bitcoin_transactions_stream"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, topic_id)

for n in range(1, 10):
    data = {"hash":str(uuid.uuid4()),"size":123,"block_hash":str(uuid.uuid4()),"block_number":123,"block_timestamp":time.time_ns()}
    data = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(future.result())

print(f"Published messages to {topic_path}.")