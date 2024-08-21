import pika
import json
import uvicorn
from fastapi import FastAPI, Query
from pymongo import MongoClient
from datetime import datetime

app = FastAPI()

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client.mqtt_db
collection = db.messages

# RabbitMQ setup
def callback(ch, method, properties, body):
    message = json.loads(body)
    message['timestamp'] = datetime.utcnow()
    collection.insert_one(message)
    print(f"Received: {message}")

def consume_messages():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='mqtt_exchange', exchange_type='topic')
    channel.queue_declare(queue='mqtt_queue')
    channel.queue_bind(exchange='mqtt_exchange', queue='mqtt_queue', routing_key='mqtt.status')

    channel.basic_consume(queue='mqtt_queue', on_message_callback=callback, auto_ack=True)
    print('Waiting for messages...')
    channel.start_consuming()

# Start consuming messages in a background thread
import threading
threading.Thread(target=consume_messages, daemon=True).start()

@app.get("/status_count/")
async def get_status_count(start_time: datetime = Query(...), end_time: datetime = Query(...)):
    pipeline = [
        {
            '$match': {
                'timestamp': {
                    '$gte': start_time,
                    '$lt': end_time
                }
            }
        },
        {
            '$group': {
                '_id': '$status',
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'_id': 1}
        }
    ]
    result = list(collection.aggregate(pipeline))
    return {item['_id']: item['count'] for item in result}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
