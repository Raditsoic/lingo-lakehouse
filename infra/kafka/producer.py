from confluent_kafka import Producer
import pandas as pd
import json

conf = {
    'bootstrap.servers': 'localhost:29092',  
    'client.id': 'producer',
    'queue.buffering.max.messages': 1000000,
}

producer = Producer(**conf)
print("Spinning up Kafka Producer...")

csv_file = open("dataset/duolingo-spaced-repetition-data.csv", "r")
data = pd.read_csv(csv_file)

topic = "duolingo"

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

for index, row in data.iterrows():
    message = {
        "user_id": row["user_id"],
        "timestamp": row["timestamp"],
        "learning_language": row["learning_language"],
        "ui_language": row["ui_language"],
        "lexeme_id": row["lexeme_id"],
        "lexeme_string": row["lexeme_string"],
        "delta": row["delta"],
        "p_recall": row["p_recall"],
        "history_seen": row["history_seen"],
        "history_correct": row["history_correct"],
        "session_seen": row["session_seen"],
        "session_correct": row["session_correct"],
    }

    producer.produce(topic, key=str(index), value=json.dumps(message), callback=delivery_report)

    print(f"Message {index} sent to Kafka")

producer.flush()
print("All messages sent.")